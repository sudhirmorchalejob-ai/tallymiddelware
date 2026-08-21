require("dotenv").config({ quiet: true });

const axios = require("axios");
const Agent = require("agentkeepalive");
const xml2js = require("xml2js");
const sax = require("sax");
const { parseStringPromise } = xml2js;
const { Pool } = require("pg");
const fs = require("fs");
const path = require("path");
const os = require("os");
const crypto = require("crypto");
const readline = require("readline");
const { exec } = require("child_process");

try {
  require("v8").setFlagsFromString("--max-old-space-size=4096");
} catch (e) {}

process.on("unhandledRejection", (err) => {
  try {
    structuredLog("error", "UNHANDLED REJECTION", {
      error: err && err.message ? err.message : String(err)
    });
  } catch {}
});

process.on("uncaughtException", (err) => {
  try {
    structuredLog("error", "UNCAUGHT EXCEPTION", {
      error: err && err.message ? err.message : String(err),
      stack: err && err.stack ? err.stack : null
    });
  } catch {}
});

const DEBUG = false;

// ============================================================
//  DEVELOPER MODE
//  Enabled ONLY via the "Developer Login" authentication option.
//  Controls log visibility (console output) exclusively — it never
//  bypasses license authentication, database authentication, Tally
//  connection, existing permissions or any sync logic.
// ============================================================
let isDeveloperMode = false;

// Technical/debug console output — printed only in Developer Mode.
// Structured file logging is unaffected and always written.
function devLog(...args) {
  if (isDeveloperMode) console.log(...args);
}

// ============================================================
//  LIVE PROGRESS — one terminal line, refreshed in place with \r
//  e.g.  "Fetching Vouchers... 500 done" -> "... 1000 done"
//  Normal mode only: Developer Mode keeps the full technical log
//  output (an inline line would collide with streaming logs).
//  Counts are real processed-record counts reported by the sync
//  flow itself — never estimated.
// ============================================================
const PROGRESS = { label: null, lastDrawn: -1 };

function progressSupported() {
  return !isDeveloperMode && process.stdout && process.stdout.isTTY;
}

function progressStart(label) {
  if (!progressSupported()) return;
  PROGRESS.label = label;
  PROGRESS.lastDrawn = -1;
  process.stdout.write(`${label}... 0 done`);
}

function progressUpdate(count) {
  if (!PROGRESS.label) return;
  const n = Number(count) || 0;
  if (n === PROGRESS.lastDrawn) return;
  PROGRESS.lastDrawn = n;
  // \r returns the cursor to the start of the same line so the previous
  // text is overwritten instead of printing thousands of new lines.
  process.stdout.write(`\r${PROGRESS.label}... ${n.toLocaleString()} done`);
}

// Finishes the inline line (newline) and reports completion.
function progressEnd(finalCount, entityLabel) {
  const hadLine = !!PROGRESS.label;
  const n = Number(finalCount) || 0;
  if (hadLine) {
    const label = PROGRESS.label;
    PROGRESS.label = null;
    process.stdout.write(`\r${label}... ${n.toLocaleString()} done\n`);
  }
  if (entityLabel && !isDeveloperMode) {
    console.log(`✅ ${entityLabel} sync completed`);
  }
}

// Terminates the current inline line WITHOUT ending progress, so warning
// or error messages print on their own clean line; the counter redraws
// automatically on the next update.
function progressBreak() {
  if (PROGRESS.label && PROGRESS.lastDrawn >= 0) {
    process.stdout.write("\n");
    PROGRESS.lastDrawn = -1;
  }
}

// ============================================================
//  HTTP AGENTS (Tally + long XML responses)
// ============================================================
// Timeouts are DISABLED by default so long Tally streams are never killed
// mid-response. Large companies can stream XML for 30+ minutes per date window,
// and axios's `timeout` is a *socket idle* timeout — if no bytes arrive for that
// long (Tally pauses, or the stream is paused for backpressure during DB writes)
// the request is aborted with "timeout of Xms exceeded". Set TALLY_TIMEOUT_MS
// (ms) to opt back in; 0 (default) means no timeout at all.
const TALLY_TIMEOUT_MS = Number(process.env.TALLY_TIMEOUT_MS) || 0;
// Socket inactivity timeout must stay >= the axios request timeout so long Tally
// responses are never killed with "socket hang up" while still streaming.
const SOCKET_TIMEOUT = TALLY_TIMEOUT_MS;
const httpAgent = new Agent({
  maxSockets: 100,
  maxFreeSockets: 10,
  timeout: SOCKET_TIMEOUT,
  freeSocketTimeout: 30000
});

const httpsAgent = new Agent.HttpsAgent({
  maxSockets: 100,
  maxFreeSockets: 10,
  timeout: SOCKET_TIMEOUT,
  freeSocketTimeout: 30000
});

axios.defaults.httpAgent = httpAgent;
axios.defaults.httpsAgent = httpsAgent;
axios.defaults.timeout = TALLY_TIMEOUT_MS;

// Tally Prime HTTP endpoint configuration (dynamic port)
const PORT_CONFIG_FILE = path.join(os.homedir(), "tally-agent-port.json");
let TALLY_PORT = 9000;
let TALLY_URL = `http://localhost:${TALLY_PORT}`;

function loadTallyPort() {
  try {
    if (fs.existsSync(PORT_CONFIG_FILE)) {
      const data = JSON.parse(fs.readFileSync(PORT_CONFIG_FILE, "utf8"));
      if (data && data.port) {
        TALLY_PORT = Number(data.port) || 9000;
        TALLY_URL = `http://localhost:${TALLY_PORT}`;
      }
    }
  } catch {
    TALLY_PORT = 9000;
    TALLY_URL = `http://localhost:9000`;
  }
}

function saveTallyPort(port) {
  try {
    TALLY_PORT = Number(port) || 9000;
    TALLY_URL = `http://localhost:${TALLY_PORT}`;
    fs.writeFileSync(PORT_CONFIG_FILE, JSON.stringify({ port: TALLY_PORT }, null, 2));
  } catch (err) {
    console.error("⚠️ Failed to save Tally port configuration:", err.message);
  }
}

loadTallyPort();

// ============================================================
//  TALLY HTTP REQUEST HELPER WITH TIMEOUT & RETRIES
// ============================================================
async function postTallyXml(xmlData, options = {}) {
  const retries = options.retries ?? 3;
  const timeout = options.timeout ?? TALLY_TIMEOUT_MS; // 0 = no timeout (never killed for being slow)

  let lastError = null;
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const res = await axios.post(TALLY_URL, xmlData, {
        headers: { "Content-Type": "text/xml" },
        timeout
      });
      return res;
    } catch (err) {
      lastError = err;
      const isTimeout = err.code === "ECONNABORTED" || (err.message && err.message.includes("timeout"));
      if (attempt < retries) {
        devLog(`  ⚠️ Tally XML request ${isTimeout ? "timed out" : "failed"} (Attempt ${attempt}/${retries}): ${err.message}. Retrying in 2s...`);
        await new Promise((r) => setTimeout(r, 2000));
      }
    }
  }
  throw lastError;
}

// Helper to split date ranges into fixed-size chunks (prevents huge Tally responses & timeouts)
function fmtYMD(d) {
  const yr = d.getFullYear();
  const mo = String(d.getMonth() + 1).padStart(2, "0");
  const dy = String(d.getDate()).padStart(2, "0");
  return `${yr}${mo}${dy}`;
}

// Fixed-size rolling date windows used for vouchers.
// Every window stays small so Tally never has to build a huge single response
// (the root cause of socket hang-ups / timeouts on large companies).
function getVoucherWindows(startDate, days = 5) {
  const now = new Date();
  const windows = [];
  const cur = new Date(startDate);
  let guard = 0;
  while (cur <= now && guard < 20000) {
    const from = new Date(cur);
    const to = new Date(cur);
    to.setDate(to.getDate() + days - 1);
    if (to > now) to.setTime(now.getTime());
    windows.push({
      fromDate: fmtYMD(from),
      toDate: fmtYMD(to),
      label: fmtYMD(from)
    });
    cur.setDate(cur.getDate() + days);
    guard++;
  }
  return windows;
}

// Annual date windows used for bills sync. Bills are few (one row per open
// invoice), so a single year per request keeps the report small without the
// request explosion of the 5-day voucher windows.
function getBillsWindows(startYear = null) {
  const now = new Date();
  const currentYear = now.getFullYear();
  const fromYear = startYear || (currentYear - 1);

  const windows = [];
  const cur = new Date(fromYear, 3, 1); // April 1st of startYear (Financial Year start)
  let guard = 0;

  while (cur <= now && guard < 100) {
    const y = cur.getFullYear();
    const firstDay = new Date(y, 3, 1);
    const lastDay = new Date(y + 1, 2, 31); // March 31st of the following year
    const fromDate = fmtYMD(firstDay);
    const toDate = lastDay > now ? fmtYMD(now) : fmtYMD(lastDay);

    windows.push({
      fromDate,
      toDate,
      label: fromDate
    });

    cur.setFullYear(y + 1);
    guard++;
  }

  return windows;
}

// Default start of the full-history window when Tally does not report a company
// "Starting From" date. Overridable via SYNC_START_YEAR (e.g. 2005).
function resolveSyncStartYear(company) {
  const env = Number(process.env.SYNC_START_YEAR);
  if (env && env > 1900 && env < 3000) return env;
  if (company && company.startingFrom && /^\d{4}-\d{2}-\d{2}$/.test(company.startingFrom)) {
    return Number(company.startingFrom.slice(0, 4));
  }
  // Default to full history so short-named companies (no "(From ...)" suffix
  // and no resolvable Starting From) never miss older vouchers/bills/ledgers.
  return 1900;
}

let pool = null;

const DB_CONFIG_FILE = path.join(os.homedir(), "tally-agent-db-config.enc");

// Derive a machine-local encryption key (so the stored file is encrypted at rest)
function getEncryptionKey() {
  const seed = `${os.hostname()}|${os.platform()}|${os.arch()}|${os.userInfo().username}`;
  return crypto.scryptSync(seed, "tally-agent-config-v1", 32);
}

function encryptConfig(obj) {
  const key = getEncryptionKey();
  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv("aes-256-gcm", key, iv);
  const data = Buffer.from(JSON.stringify(obj), "utf8");
  const enc = Buffer.concat([cipher.update(data), cipher.final()]);
  const tag = cipher.getAuthTag();
  return JSON.stringify({
    v: 1,
    iv: iv.toString("base64"),
    tag: tag.toString("base64"),
    data: enc.toString("base64")
  });
}

function decryptConfig(raw) {
  const key = getEncryptionKey();
  const parsed = JSON.parse(raw);
  const decipher = crypto.createDecipheriv(
    "aes-256-gcm",
    key,
    Buffer.from(parsed.iv, "base64")
  );
  decipher.setAuthTag(Buffer.from(parsed.tag, "base64"));
  const dec = Buffer.concat([
    decipher.update(Buffer.from(parsed.data, "base64")),
    decipher.final()
  ]);
  return JSON.parse(dec.toString("utf8"));
}

function saveDbConfig(cfg) {
  fs.writeFileSync(DB_CONFIG_FILE, encryptConfig(cfg));
}

function loadDbConfig() {
  if (!fs.existsSync(DB_CONFIG_FILE)) return null;
  try {
    return decryptConfig(fs.readFileSync(DB_CONFIG_FILE, "utf8"));
  } catch (err) {
    console.log("⚠️ Could not decrypt saved config. Starting wizard.");
    return null;
  }
}

function buildPoolConfig(cfg) {
  if (cfg.connectionString) {
    return {
      connectionString: cfg.connectionString,
      max: 20,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000
    };
  }
  return {
    host: cfg.host || "localhost",
    port: Number(cfg.port) || 5432,
    user: cfg.user,
    password: cfg.password,
    database: cfg.database,
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000
  };
}

async function testDbConnection(cfg) {
  const testPool = new Pool(buildPoolConfig(cfg));
  try {
    await testPool.query("SELECT 1");
    return true;
  } catch (err) {
    return false;
  } finally {
    await testPool.end();
  }
}

async function connectWithConfig(cfg) {
  if (pool) {
    await pool.end().catch(() => {});
  }
  pool = new Pool(buildPoolConfig(cfg));
  await initDb();
}

/* --------------------------
   DATABASE CONFIG WIZARD
---------------------------- */
const BACK_SIGNAL = Symbol("BACK");

// Prompts for one wizard field; typing B throws BACK_SIGNAL so the wizard
// can step back to its previous screen.
async function askField(query, hideInput = false) {
  const answer = (await askQuestion(query, hideInput)).trim();
  if (answer.toLowerCase() === "b") throw BACK_SIGNAL;
  return answer;
}

async function dbWizard() {
  console.log("\n🛠️  Database Configuration Wizard");
  console.log("======================================");
  console.log("ℹ️ Type B at any prompt to go back.\n");

  while (true) {
    const modeInput = await askQuestion(
      "\nDatabase type — [1] Local PostgreSQL, [2] External PostgreSQL, [B] Back: "
    );
    const modeChoice = modeInput.trim();

    if (modeChoice.toLowerCase() === "b") {
      console.log("↩️ Back.");
      return null;
    }
    if (modeChoice !== "1" && modeChoice !== "2") {
      console.log("⚠️ Invalid option. Please choose 1, 2, or B.");
      continue;
    }

    let cfg;
    try {
      cfg = { mode: modeChoice === "2" ? "external" : "local" };

      if (modeChoice === "2") {
        const useUrl = (await askField("Use a connection URL? [y/N]: ")).toLowerCase();
        if (useUrl === "y") {
          cfg.connectionString = await askField(
            "Connection URL (postgres://user:pass@host:port/db): "
          );
        } else {
          cfg.host = await askField("Host: ");
          cfg.port = (await askField("Port [5432]: ")) || "5432";
          cfg.database = await askField("Database name: ");
          cfg.user = await askField("Username: ");
          cfg.password = await askField("Password: ", true);
        }
      } else {
        cfg.host = (await askField("Host [localhost]: ")) || "localhost";
        cfg.port = (await askField("Port [5432]: ")) || "5432";
        cfg.database = await askField("Database name: ");
        cfg.user = await askField("Username: ");
        cfg.password = await askField("Password: ", true);
      }
    } catch (err) {
      if (err === BACK_SIGNAL) {
        console.log("\n↩️ Back to database type selection.");
        continue;
      }
      throw err;
    }

    console.log("\n🔌 Testing connection...");
    if (await testDbConnection(cfg)) {
      console.log("✅ Connection successful!");
      saveDbConfig(cfg);
      console.log("🔐 Configuration saved securely (encrypted) for next launch.\n");
      return cfg;
    }

    console.log("❌ Connection failed. Please check the details and try again.\n");
  }
}

/* --------------------------
   AUTHENTICATION & BROWSER HELPERS
---------------------------- */
let AUTH_TOKEN = null;
let ADMIN_EMAIL = null;
const BACKEND_URL = process.env.BACKEND_URL || "https://tally-connect-yga1.onrender.com";

/**
 * Opens a URL in the operating system's default web browser.
 * Compatible with Node.js, pkg compiled executable, Windows, macOS, and Linux.
 * @param {string} url - The URL to open.
 * @returns {Promise<void>}
 */
function openDefaultBrowser(url) {
  return new Promise((resolve, reject) => {
    let command;
    if (process.platform === "win32") {
      command = `start "" "${url}"`;
    } else if (process.platform === "darwin") {
      command = `open "${url}"`;
    } else {
      command = `xdg-open "${url}"`;
    }

    exec(command, (error) => {
      if (error) {
        reject(error);
      } else {
        resolve();
      }
    });
  });
}

/**
 * Handles the Forgot Password flow by redirecting the user to the web portal.
 */
async function handleForgotPassword() {
  const resetUrl = "https://tally-connect.com/forgot-password";
  try {
    await openDefaultBrowser(resetUrl);
    console.log("\nOpening your default browser...\n");
    console.log("Redirecting to:\n");
    console.log(`${resetUrl}\n`);
    console.log("Please reset your password using the web portal.\n");
    console.log("After resetting your password, return to this terminal and log in again.\n");
  } catch (err) {
    console.log("\nUnable to open your browser automatically.\n");
    console.log("Please manually visit:\n");
    console.log(`${resetUrl}\n`);
  }
  await askQuestion("Press Enter to continue...");
}

/**
 * Displays the Authentication Menu.
 */
async function showAuthenticationMenu() {
  console.log("\n========================================");
  console.log("       Tally-Connect Authentication");
  console.log("========================================");
  console.log("");
  console.log("1. Login");
  console.log("2. Developer Login");
  console.log("3. Forgot Password");
  console.log("4. Exit");
  console.log("");
  const choice = (await askQuestion("Select an option: ")).trim();
  return choice;
}

const LICENSE_BASE_URL = "https://dashboard.licentic.org";
const LICENSE_PRODUCT_ID = "695902cfc240b17f16c3d716";

/**
 * Authenticates the user strictly against the License System using email & password,
 * then verifies the user's active license and receives JWT token.
 */
async function agentLogin() {
  console.log("\n🔐 License Authentication");
  console.log("========================================");
  const email = (await askQuestion("Enter admin email (B = back): ")).trim();

  if (email.toLowerCase() === "b") {
    console.log("\n↩️ Back to authentication menu.\n");
    return false;
  }

  const password = await askQuestion("Enter admin password: ", true);

  if (!email || !password) {
    console.log("⚠️ Email and password cannot be empty.");
    return false;
  }

  console.log("\n🔍 Authenticating credentials with License System...");

  try {
    // Step 1: Validate Email & Password against License System
    const loginRes = await axios.post(
      `${LICENSE_BASE_URL}/api/auth/login`,
      { email, password },
      { timeout: 15000 }
    );

    if (!loginRes.data?.success || !loginRes.data?.token) {
      console.log(`\n❌ License Authentication Failed: ${loginRes.data?.message || "Authentication failed."}`);
      return false;
    }

    const jwtToken = loginRes.data.token;
    const userObj = loginRes.data.user;

    console.log("🔍 Verifying active license...");

    // Step 2: Verify Active License
    const licenseUrl = `${LICENSE_BASE_URL}/api/external/actve-license/${encodeURIComponent(email)}?productId=${LICENSE_PRODUCT_ID}`;
    const licenseRes = await axios.get(licenseUrl, { timeout: 15000 });

    const licenseData = licenseRes.data;
    const activeLicense = licenseData?.activeLicense;
    const expiryInfo = licenseData?.expiryInfo;

    if (!activeLicense || expiryInfo?.isExpired) {
      console.log(`\n❌ License Verification Failed: No active license found for '${email}'.`);
      if (expiryInfo?.isExpired) {
        console.log(`⚠️ Reason: License expired on ${expiryInfo?.endDate ? new Date(expiryInfo.endDate).toLocaleDateString() : "unknown date"}.`);
      }
      return false;
    }

    const planName = activeLicense.licenseTypeId?.name || "Standard";
    const licenseKey = activeLicense.licenseKey || "N/A";
    const endDateRaw = activeLicense.validUntil || activeLicense.endDate || expiryInfo?.endDate;
    const validUntil = endDateRaw ? new Date(endDateRaw).toLocaleDateString() : "N/A";

    ADMIN_EMAIL = email;
    AUTH_TOKEN = jwtToken;

    console.log("\n✅ License Authenticated Successfully!");
    console.log(`👤 Admin: ${userObj?.name || email} (${email})`);
    console.log(`🔑 License Key: ${licenseKey}`);
    console.log(`📦 License Plan: ${planName}`);
    console.log(`📅 Valid Until: ${validUntil}\n`);
    return true;

  } catch (err) {
    if (err.response?.data?.message) {
      console.log(`\n❌ License Authentication Failed: ${err.response.data.message}`);
    } else if (err.response?.data?.errors && Array.isArray(err.response.data.errors)) {
      console.log(`\n❌ License Authentication Failed: ${err.response.data.errors.join(", ")}`);
    } else {
      console.log(`\n❌ License Authentication Failed: Could not connect to License System (${err.message}).`);
    }
    return false;
  }
}

/**
 * Developer Login — enables Developer Mode (log visibility only).
 * Does NOT authenticate the license flow and does NOT bypass any
 * existing authentication; the normal login is still required.
 * Credentials can be overridden via DEVELOPER_EMAIL / DEVELOPER_PASSWORD env.
 */
async function developerLogin() {
  console.log("\nDeveloper Login");
  console.log("========================================");
  const email = (await askQuestion("Enter developer email (B = back): ")).trim();

  if (email.toLowerCase() === "b") {
    console.log("\n↩️ Back to authentication menu.\n");
    return false;
  }

  const password = await askQuestion("Enter developer password: ", true);

  const devEmail = process.env.DEVELOPER_EMAIL || "sudhir@gmail.com";
  const devPassword = process.env.DEVELOPER_PASSWORD || "sudhirRA@2026";

  if (email === devEmail && password === devPassword) {
    isDeveloperMode = true;
    console.log("\n✅ Developer authentication successful.");
    console.log("🔧 Developer Mode enabled.\n");
    return true;
  }

  console.log("\n❌ Invalid developer credentials.");
  return false;
}

/**
 * Runs the Authentication Flow loop until successful login or exit.
 */
async function startAuthenticationFlow() {
  let authenticated = false;

  while (!authenticated) {
    const choice = await showAuthenticationMenu();

    if (choice === "1") {
      const loginSuccess = await agentLogin();
      if (loginSuccess) {
        authenticated = true;
      }
    } else if (choice === "2") {
      await developerLogin();
    } else if (choice === "3") {
      await handleForgotPassword();
    } else if (choice === "4") {
      console.log("\n👋 Exiting TallyScrapper. Goodbye!");
      process.exit(0);
    } else {
      console.log("\n⚠️ Invalid option. Please select 1, 2, 3, or 4.");
    }
  }
}

/* --------------------------
   TALLY PORT CONFIG WIZARD
---------------------------- */
async function tallyPortWizard() {
  console.log("\n⚙️  Tally HTTP Port Configuration");
  console.log("======================================");
  console.log(`Current Tally Port: ${TALLY_PORT} (Endpoint: ${TALLY_URL})\n`);

  const input = await askQuestion(`Enter new Tally HTTP Port [${TALLY_PORT}] (B = back): `);
  const trimmed = input.trim();

  if (trimmed.toLowerCase() === "b") {
    console.log("↩️ Back to main menu.\n");
    return;
  }

  if (!trimmed) {
    console.log(`ℹ️ Tally Port kept as: ${TALLY_PORT}\n`);
    return;
  }

  const newPort = parseInt(trimmed, 10);
  if (isNaN(newPort) || newPort < 1 || newPort > 65535) {
    console.log("❌ Invalid port number. Port must be between 1 and 65535.\n");
    return;
  }

  saveTallyPort(newPort);
  console.log(`✅ Tally HTTP Port updated successfully to: ${TALLY_PORT}`);
  console.log(`📡 New Tally Endpoint: ${TALLY_URL}\n`);
}

/* --------------------------
   MAIN MENU
---------------------------- */
async function mainMenu() {
  console.log("\n================= TALLY AGENT =================");
  console.log("  [1] Start Sync");
  console.log("  [2] Database Settings (Change Database Configuration)");
  console.log("  [3] Tally Port Settings (Change Tally HTTP Port)");
  console.log("  [4] Reset & Full Re-sync (clears all checkpoints)");
  console.log("  [5] Exit");
  console.log("=================================================");

  const choice = (await askQuestion("\nSelect an option: ")).trim();
  return choice;
}

// ============================================================
//  APP STATE
// ============================================================
let ACTIVE_COMPANY_GUID = null;
let ACTIVE_COMPANY_NAME = null;
let SYNC_INTERVAL = 0; // ms, set by user
let isSyncRunning = false;
const STATE_FILE = path.join(os.homedir(), "tally-agent-state.json");

// ============================================================
//  HELPERS
// ============================================================
function askQuestion(query, hideInput = false) {
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });

  if (hideInput) {
    process.stdout.write(query);
    process.stdin.setRawMode(true);
    let input = "";

    process.stdin.on("data", (char) => {
      char = char.toString();
      if (char === "\n" || char === "\r") {
        process.stdin.setRawMode(false);
        rl.close();
        console.log();
        rl.removeAllListeners();
        return;
      }
      if (char === "\u0003") process.exit();
      input += char;
    });

    return new Promise((resolve) => {
      rl.on("close", () => resolve(input));
    });
  }

  return new Promise((resolve) =>
    rl.question(query, (answer) => {
      rl.close();
      resolve(answer);
    })
  );
}

function normalizeText(value) {
  if (!value) return null;
  if (Array.isArray(value)) return value.join(" ");
  if (typeof value === "object" && value._) return value._;
  return String(value);
}

function extractAddressValue(addr) {
  if (!addr) return null;
  if (Array.isArray(addr)) {
    return addr
      .map((a) => (typeof a === "object" ? a._ : a))
      .filter(Boolean)
      .join(" ");
  }
  if (typeof addr === "object" && addr._) return addr._;
  return String(addr);
}

function extractPhone(text) {
  const str = normalizeText(text);
  if (!str) return null;
  const matches = str.match(/\b[6-9]\d{9}\b/g);
  return matches ? matches[0] : null;
}

function extractEmail(text) {
  const str = normalizeText(text);
  if (!str) return null;
  const match = str.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  return match ? match[0] : null;
}

function parseTallyNumber(val = "0") {
  if (val === null || val === undefined) return 0;
  if (typeof val === "object") {
    if (val._ !== undefined) val = val._;
    else return 0;
  }
  val = String(val);
  return Number(
    val
      .replace(/\(/g, "-")
      .replace(/\)/g, "")
      .replace(/,/g, "")
      .replace(/[^0-9.-]/g, "")
  );
}

// Convert "20250401" (Tally) -> "2025-04-01" (ISO)
function toISODate(v) {
  if (!v) return null;
  const s = String(v).trim();
  if (/^\d{8}$/.test(s)) return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
  const d = new Date(s);
  return isNaN(d.getTime()) ? null : s;
}

// Deterministic hash to keep event-style records duplicate-free
function makeHash(...parts) {
  return crypto
    .createHash("sha256")
    .update(parts.map((p) => String(p ?? "")).join("|"))
    .digest("hex");
}

function loadState() {
  try {
    if (!fs.existsSync(STATE_FILE)) return {};
    return JSON.parse(fs.readFileSync(STATE_FILE, "utf8"));
  } catch {
    return {};
  }
}

function saveState(state) {
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

// ============================================================
//  CHECKPOINT / RESUME STATE (per-company, atomic on-disk writes)
//  Lets interrupted syncs continue from the last completed window.
// ============================================================
const SYNC_STATE_FILE = path.join(os.homedir(), "tally-agent-sync-state.json");

// Bump whenever the request/parser semantics change so old resume checkpoints
// (which were written with a different fetch path) are treated as invalid.
const SYNC_SCHEMA = 3;

function loadSyncState() {
  try {
    if (!fs.existsSync(SYNC_STATE_FILE)) return { companies: {} };
    return JSON.parse(fs.readFileSync(SYNC_STATE_FILE, "utf8"));
  } catch {
    return { companies: {} };
  }
}

function saveSyncState(state) {
  try {
    ensureLogDir();
    const tmp = `${SYNC_STATE_FILE}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(state, null, 2));
    fs.renameSync(tmp, SYNC_STATE_FILE);
  } catch (err) {
    structuredLog("warn", "Failed to persist sync state", { error: err.message });
  }
}

function getCompanyCheckpoint(companyGuid) {
  const state = loadSyncState();
  const c = (state.companies && state.companies[companyGuid]) || {};
  for (const entity of Object.keys(c)) {
    if (c[entity] && c[entity].schema !== SYNC_SCHEMA) {
      c[entity] = { lastWindow: null, startYear: c[entity].startYear || null, schema: SYNC_SCHEMA };
    }
  }
  return c;
}

// Marks a date window (label = YYYYMMDD of the window start) as fully synced.
// `startYear` is recorded so a later change to SYNC_START_YEAR or the company
// start date invalidates the old resume point instead of skipping unsynced data.
function setCompanyCheckpoint(companyGuid, entity, windowLabel, startYear) {
  const state = loadSyncState();
  state.companies = state.companies || {};
  const c = (state.companies[companyGuid] = state.companies[companyGuid] || {});
  c[entity] = {
    lastWindow: windowLabel,
    startYear: startYear || null,
    schema: SYNC_SCHEMA,
    updatedAt: new Date().toISOString()
  };
  saveSyncState(state);
}

// Per-entity bill resume state (RECEIVABLE/PAYABLE). Bills previously had no
// checkpoint, so the first run after this change always does a full (windowed)
// sync, then resumes from the last completed window like every other entity.
function getBillSyncState(billType) {
  const key = billType === "PAYABLE" ? "bills_payable" : "bills_receivable";
  const checkpoint = getCompanyCheckpoint(ACTIVE_COMPANY_GUID)[key] || {};
  return {
    lastWindow: checkpoint.lastWindow || null,
    startYear: checkpoint.startYear || null,
    schema: checkpoint.schema || null
  };
}

function saveBillSyncState(billType, windowLabel, startYear) {
  const key = billType === "PAYABLE" ? "bills_payable" : "bills_receivable";
  setCompanyCheckpoint(ACTIVE_COMPANY_GUID, key, windowLabel, startYear);
}

// ============================================================
//  SYNC STATE (DB-backed incremental sync metadata)
// ============================================================
async function getSyncState(companyGuid, entity) {
  try {
    const { rows } = await pool.query(
      "SELECT * FROM sync_state WHERE company_guid = $1 AND entity = $2",
      [companyGuid, entity]
    );
    return rows[0] || null;
  } catch {
    return null;
  }
}

async function upsertSyncState(companyGuid, entity, { lastSyncAt, lastWindow, recordCount, checksum, meta } = {}) {
  const existing = await getSyncState(companyGuid, entity);
  const now = new Date().toISOString();
  if (existing) {
    const fields = [];
    const vals = [];
    let idx = 3;
    if (lastSyncAt !== undefined) { fields.push(`last_sync_at = $${idx++}`); vals.push(lastSyncAt); }
    if (lastWindow !== undefined) { fields.push(`last_window = $${idx++}`); vals.push(lastWindow); }
    if (recordCount !== undefined) { fields.push(`record_count = $${idx++}`); vals.push(recordCount); }
    if (checksum !== undefined) { fields.push(`checksum = $${idx++}`); vals.push(checksum); }
    if (meta !== undefined) { fields.push(`meta = $${idx++}`); vals.push(JSON.stringify(meta)); }
    if (fields.length === 0) return;
    vals.unshift(companyGuid, entity);
    await pool.query(`UPDATE sync_state SET ${fields.join(", ")} WHERE company_guid = $1 AND entity = $2`, vals);
  } else {
    await pool.query(
      `INSERT INTO sync_state (company_guid, entity, last_sync_at, last_window, record_count, checksum, meta)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [companyGuid, entity, lastSyncAt || now, lastWindow || null, recordCount || 0, checksum || null, JSON.stringify(meta || {})]
    );
  }
}

function computeGuidChecksum(guids) {
  if (!guids.length) return null;
  const sorted = [...guids].sort();
  let hash = 0;
  for (const g of sorted) {
    for (let i = 0; i < g.length; i++) {
      hash = ((hash << 5) - hash + g.charCodeAt(i)) | 0;
    }
  }
  return hash.toString(36);
}

// ============================================================
//  STRUCTURED LOGGING & SAFE DB WRITES
// ============================================================
const LOG_DIR = path.join(os.homedir(), "tally-agent-logs");
let SYNC_RUN_ID = null;

function ensureLogDir() {
  try {
    fs.mkdirSync(LOG_DIR, { recursive: true });
  } catch {}
}

function todayStamp() {
  return new Date().toISOString().slice(0, 10);
}

function logFilePath(prefix) {
  return path.join(LOG_DIR, `${prefix}-${todayStamp()}.log`);
}

function writeJsonLine(file, entry) {
  ensureLogDir();
  try {
    fs.appendFileSync(file, JSON.stringify(entry) + "\n");
  } catch {}
}

function structuredLog(level, msg, meta = {}) {
  const entry = {
    ts: new Date().toISOString(),
    level,
    msg,
    syncId: SYNC_RUN_ID,
    ...meta
  };
  writeJsonLine(logFilePath("sync"), entry);
  // Detailed technical logs are shown on console ONLY in Developer Mode.
  // They are ALWAYS written to the log file regardless of mode.
  if (!isDeveloperMode) return;
  const suffix = Object.keys(meta).length ? " " + JSON.stringify(meta) : "";
  console.log(`[${level.toUpperCase()}] ${msg}${suffix}`);
}

function logFailure(entity, identifier, error, meta = {}) {
  const entry = {
    ts: new Date().toISOString(),
    syncId: SYNC_RUN_ID,
    entity,
    identifier,
    error: (error && (error.message || error)) || String(error),
    stack: (error && error.stack) || null,
    ...meta
  };
  writeJsonLine(logFilePath("failures"), entry);
  structuredLog("error", `[${entity}] Failed record: ${identifier}`, {
    error: entry.error
  });
}

function formatDuration(ms) {
  if (ms < 1000) return `${ms}ms`;
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(2)}s`;
  return `${Math.floor(s / 60)}m ${Math.round(s % 60)}s`;
}

function fmtBytes(n) {
  if (!n) return "0 B";
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function summarizeRecord(record) {
  if (!record || typeof record !== "object") return record;
  const { payload, ...rest } = record;
  return rest;
}

const SYNC_SUMMARY = {
  startedAt: null,
  endedAt: null,
  companiesSynced: 0,
  companyGuids: [],
  entities: {},
  voucherTypeBreakdown: null,
  companies: [] // per-company summary buckets: { guid, name, entities, voucherTypeBreakdown }
};

// Points at the bucket of the company currently being synced so entity
// stats are recorded both in the aggregate and per-company views.
let CURRENT_COMPANY_STATS = null;

function recordEntityStats(entity, { fetched = 0, saved = 0, failed = 0, skipped = 0, newCount = null } = {}) {
  const applyTo = (bucket) => {
    const prev = bucket[entity] || { fetched: 0, saved: 0, failed: 0, skipped: 0, newCount: 0 };
    bucket[entity] = {
      fetched: prev.fetched + fetched,
      saved: prev.saved + saved,
      failed: prev.failed + failed,
      skipped: prev.skipped + skipped,
      // Real newly-inserted count from the upsert result; falls back to
      // saved only when a caller does not provide it.
      newCount: prev.newCount + (newCount === null ? saved : newCount)
    };
  };
  applyTo(SYNC_SUMMARY.entities);
  if (CURRENT_COMPANY_STATS) applyTo(CURRENT_COMPANY_STATS.entities);
}

// Friendly display labels for entities in the final summary.
function entityLabel(entityKey) {
  const found = DATA_TYPES.find((d) => d.key === entityKey);
  return found ? found.label : entityKey;
}

async function printSyncSummary(syncStart) {
  const dur = Date.now() - syncStart;
  structuredLog("info", "Sync summary", {
    startedAt: SYNC_SUMMARY.startedAt ? SYNC_SUMMARY.startedAt.toISOString() : null,
    endedAt: SYNC_SUMMARY.endedAt ? SYNC_SUMMARY.endedAt.toISOString() : null,
    durationMs: dur,
    duration: formatDuration(dur),
    companies: SYNC_SUMMARY.companiesSynced,
    entities: SYNC_SUMMARY.entities
  });

  // One completely separate summary block per company (never mixed).
  const companyBlocks = SYNC_SUMMARY.companies.length
    ? SYNC_SUMMARY.companies
    : [{ guid: null, name: null, entities: SYNC_SUMMARY.entities, voucherTypeBreakdown: SYNC_SUMMARY.voucherTypeBreakdown }];
  const multipleCompanies = companyBlocks.length > 1;

  for (let ci = 0; ci < companyBlocks.length; ci++) {
    const cs = companyBlocks[ci];
    const guids = cs.guid ? [cs.guid] : SYNC_SUMMARY.companyGuids;

    // In-DB counts for THIS company only (source of truth stays the DB).
    let dbTotals = {};
    try {
      if (pool && guids.length > 0) {
        const res = await pool.query(
          `SELECT
             (SELECT count(*)::int FROM vouchers WHERE company_guid = ANY($1)) AS vouchers,
             (SELECT count(*)::int FROM invoices WHERE company_guid = ANY($1)) AS invoices,
             (SELECT count(*)::int FROM ledgers WHERE company_guid = ANY($1)) AS ledgers,
             (SELECT count(*)::int FROM orders WHERE company_guid = ANY($1)) AS orders,
             (SELECT count(*)::int FROM inventory_items WHERE company_guid = ANY($1)) AS inventory_items,
             (SELECT count(*)::int FROM bills WHERE company_guid = ANY($1) AND bill_type = 'RECEIVABLE') AS bills_receivable,
             (SELECT count(*)::int FROM bills WHERE company_guid = ANY($1) AND bill_type = 'PAYABLE') AS bills_payable`,
          [guids]
        );
        dbTotals = res.rows[0] || {};
      }
    } catch (e) {
      dbTotals = {};
    }

    console.log("");
    console.log(multipleCompanies ? `========== COMPANY ${ci + 1} SUMMARY ==========` : "========== SYNC SUMMARY ==========");
    if (cs.name) console.log(`Company : ${cs.name}`);
    if (isDeveloperMode) {
      console.log(`Started   : ${SYNC_SUMMARY.startedAt ? SYNC_SUMMARY.startedAt.toISOString() : "N/A"}`);
      console.log(`Ended     : ${SYNC_SUMMARY.endedAt ? SYNC_SUMMARY.endedAt.toISOString() : "N/A"}`);
      console.log(`Duration  : ${formatDuration(dur)}`);
      console.log(`Companies : ${SYNC_SUMMARY.companiesSynced}`);
    }
    console.log("");
    console.log(
      "Entity".padEnd(26) + " " + "Fetched".padStart(7) + " " + "New".padStart(6) +
      " " + "Failed".padStart(8) + " " + "Skipped".padStart(8) + " " + "In DB".padStart(8)
    );
    console.log("-".repeat(72));

    // Entities relevant to the selected sync only ("companies" detection is
    // internal bookkeeping and not shown).
    const displayEntities = Object.entries(cs.entities).filter(([entity]) => entity !== "companies");
    let totalNewRecords = 0;
    for (const [entity, s] of displayEntities) {
      const total = dbTotals[entity] !== undefined ? dbTotals[entity] : "-";
      totalNewRecords += s.newCount || 0;
      console.log(
        `${entityLabel(entity).padEnd(26)} ${String(s.fetched).padStart(7)} ${String(s.newCount || 0).padStart(6)} ${String(s.failed).padStart(8)} ${String(s.skipped).padStart(8)} ${String(total).padStart(8)}`
      );
    }

    // 🆕 New Records section — real newly-inserted counts of THIS cycle.
    if (displayEntities.length > 0) {
      console.log("");
      console.log("🆕 NEW RECORDS FOUND");
      for (const [entity, s] of displayEntities) {
        console.log(`   • ${entityLabel(entity)}: ${s.newCount || 0} new`);
      }
      console.log("");
      console.log(`📊 Total New Records: ${totalNewRecords}`);
      if (totalNewRecords === 0) {
        console.log("");
        console.log("ℹ️ No new records found. Existing data is up to date.");
      }
    }

    // Voucher Type Breakdown — ALWAYS query DB for active+cancelled counts.
    // With is_cancelled column, both counts come directly from the DB (source of truth).
    // Queried per company so multi-company summaries never mix data.
    try {
      if (pool && guids.length > 0) {
        const { rows: dbRows } = await pool.query(
          `SELECT COALESCE(voucher_type, 'Unknown') AS voucher_type,
                  count(*) FILTER (WHERE NOT is_cancelled)::int AS active,
                  count(*) FILTER (WHERE is_cancelled)::int AS cancelled
           FROM vouchers WHERE company_guid = ANY($1)
           GROUP BY voucher_type ORDER BY active DESC, cancelled DESC`,
          [guids]
        );

        if (dbRows.length > 0) {
          console.log("");
          console.log("Voucher Type Breakdown (Active / Cancelled / Total):");
          let gA = 0, gC = 0, gT = 0;
          for (const r of dbRows) {
            gA += r.active; gC += r.cancelled; gT += r.active + r.cancelled;
            console.log(
              `  • ${r.voucher_type}: ${r.active.toLocaleString()} / ${r.cancelled.toLocaleString()} / ${(r.active + r.cancelled).toLocaleString()}`
            );
          }
          console.log(`  ── TOTAL: ${gA.toLocaleString()} / ${gC.toLocaleString()} / ${gT.toLocaleString()}`);
        } else if (cs.voucherTypeBreakdown && cs.voucherTypeBreakdown.length > 0) {
          // Fallback: last-known breakdown captured during this cycle.
          console.log("");
          console.log("Voucher Type Breakdown (Active / Cancelled / Total):");
          let gA = 0, gC = 0, gT = 0;
          for (const r of cs.voucherTypeBreakdown) {
            gA += r.active; gC += r.cancelled; gT += r.total;
            console.log(`  • ${r.type}: ${r.active.toLocaleString()} / ${r.cancelled.toLocaleString()} / ${r.total.toLocaleString()}`);
          }
          console.log(`  ── TOTAL: ${gA.toLocaleString()} / ${gC.toLocaleString()} / ${gT.toLocaleString()}`);
        }
      }
    } catch (e) {}

    console.log("");
    console.log("========================================");
  }

  // Incremental sync state — technical checkpoint details, Developer Mode only.
  if (isDeveloperMode && pool && SYNC_SUMMARY.companyGuids.length > 0) {
    try {
      const { rows: stateRows } = await pool.query(
        `SELECT entity, last_sync_at, record_count, meta FROM sync_state
         WHERE company_guid = ANY($1) ORDER BY entity`,
        [SYNC_SUMMARY.companyGuids]
      );
      if (stateRows.length > 0) {
        console.log("");
        console.log("Incremental Sync State:");
        for (const r of stateRows) {
          const lastSync = r.last_sync_at ? new Date(r.last_sync_at).toLocaleString() : "never";
          const meta = r.meta || {};
          const changed = meta.changed === true ? " ⚡ changed" : meta.changed === false ? " (unchanged)" : "";
          const windows = meta.windowsProcessed !== undefined
            ? ` [${meta.windowsProcessed} windows${meta.windowsResumed ? ", " + meta.windowsResumed + " resumed" : ""}${meta.windowsReFetchedRecent ? ", " + meta.windowsReFetchedRecent + " recent" : ""}]`
            : "";
          console.log(`  • ${r.entity}: last sync ${lastSync}, ${r.record_count} records${changed}${windows}`);
        }
      }
    } catch {}
  }
}

// ============================================================
//  TALLY FETCH WRAPPER (retries + structured logs)
// ============================================================
async function fetchTallyXml(xmlData, { entity = "tally", timeout = TALLY_TIMEOUT_MS } = {}) {
  const t0 = Date.now();
  structuredLog("info", `[${entity}] FETCH: requesting data from Tally`);

  const heartbeatMs = 5000;
  const heartbeat = setInterval(() => {
    const elapsed = formatDuration(Date.now() - t0);
    const timeoutTxt = timeout ? ` (timeout ${Math.round(timeout / 1000)}s)` : "";
    devLog(`[INFO] [${entity}] FETCH: still waiting for Tally... ${elapsed} elapsed${timeoutTxt}`);
    const entry = {
      ts: new Date().toISOString(),
      level: "info",
      msg: `[${entity}] FETCH: still waiting for Tally... ${elapsed} elapsed`,
      syncId: SYNC_RUN_ID
    };
    writeJsonLine(logFilePath("sync"), entry);
  }, heartbeatMs);
  if (heartbeat.unref) heartbeat.unref();

  try {
    const res = await postTallyXml(xmlData, { timeout });
    structuredLog("info", `[${entity}] FETCH: response received`, {
      duration: formatDuration(Date.now() - t0),
      size: fmtBytes(res && res.data ? res.data.length : 0)
    });
    return res;
  } catch (err) {
    structuredLog("error", `[${entity}] FETCH: failed`, {
      error: err.message,
      duration: formatDuration(Date.now() - t0)
    });
    throw err;
  } finally {
    clearInterval(heartbeat);
  }
}

// ============================================================
//  STREAMING VOUCHER FETCH (memory-safe for very large data)
//  Reads Tally's response chunk-by-chunk through a SAX parser,
//  emitting one VOUCHER at a time. No DOM tree is ever built, so
//  memory stays flat no matter the response size (300MB, 1GB, ...).
// ============================================================
function saxVoucherStream(onVoucher) {
  const stack = [];
  const parser = sax.parser(true, { trim: true });
  const drained = [];
  parser.seenLineError = false;

  function buildValue(el) {
    if (!el.children.length) return (el.text || "").trim();
    const obj = {};
    const groups = {};
    for (const child of el.children) {
      (groups[child.name] = groups[child.name] || []).push(child);
    }
    for (const name of Object.keys(groups)) {
      obj[name] = groups[name].map((c) => buildValue(c));
    }
    return obj;
  }

  parser.onopentag = (node) => {
    if (node.name === "LINEERROR") parser.seenLineError = true;
    stack.push({ name: node.name, children: [], text: "" });
  };

  parser.ontext = (t) => {
    if (stack.length && t) {
      const top = stack[stack.length - 1];
      if (!top.children.length) top.text += t;
    }
  };

  parser.onclosetag = () => {
    const el = stack.pop();
    const parent = stack[stack.length - 1];
    if (!parent) return;
    if (el.name === "VOUCHER") {
      drained.push(buildValue(el));
    } else {
      parent.children.push(el);
    }
  };

  parser.onerror = () => {
    try {
      parser.resume();
    } catch {}
  };

  // Returns the vouchers completed since the last call (clears the buffer).
  parser.consumeDrained = () => {
    if (!drained.length) return [];
    const items = drained.slice();
    drained.length = 0;
    return items;
  };

  return parser;
}

// SAX parser for the "Bills Receivable"/"Bills Payable" report XML. Tally emits
// bill data as PARALLEL arrays (BILLFIXED[i], BILLCL[i], BILLDUE[i],
// BILLOVERDUE[i] for the i-th bill). We collect each tag into its own array —
// this works no matter whether Tally interleaves the tags per bill or groups all
// BILLFIXED first — and zip them by index afterwards. Tag names are matched at
// ANY nesting depth (top-level under ENVELOPE or nested under BODY/DATA), so the
// parser is robust to Tally layout differences. Memory stays flat: only the
// bill elements are kept, never a full DOM tree of the response.
function saxBillsStream() {
  const stack = [];
  const parser = sax.parser(true, { trim: true });
  const result = { fixed: [], cl: [], due: [], over: [] };
  parser.seenLineError = false;

  function buildValue(el) {
    if (!el.children.length) return (el.text || "").trim();
    const obj = {};
    const groups = {};
    for (const child of el.children) {
      (groups[child.name] = groups[child.name] || []).push(child);
    }
    for (const name of Object.keys(groups)) {
      obj[name] = groups[name].map((c) => buildValue(c));
    }
    return obj;
  }

  parser.onopentag = (node) => {
    if (node.name === "LINEERROR") parser.seenLineError = true;
    stack.push({ name: node.name, children: [], text: "" });
  };

  parser.ontext = (t) => {
    if (stack.length && t) {
      const top = stack[stack.length - 1];
      if (!top.children.length) top.text += t;
    }
  };

  parser.onclosetag = () => {
    const el = stack.pop();
    const parent = stack[stack.length - 1];
    if (el.name === "BILLFIXED") {
      result.fixed.push(buildValue(el));
      return;
    }
    if (el.name === "BILLCL") {
      result.cl.push(buildValue(el));
      return;
    }
    if (el.name === "BILLDUE") {
      result.due.push(buildValue(el));
      return;
    }
    if (el.name === "BILLOVERDUE") {
      result.over.push(buildValue(el));
      return;
    }
    if (parent) parent.children.push(el);
  };

  parser.onerror = () => {
    try {
      parser.resume();
    } catch {}
  };

  return { parser, result };
}

// Streams the Bills Receivable/Payable report from Tally through a SAX parser
// and returns the zipped bill records (fixed + cl + due + over). Bounded to a
// single date window by the caller, so Tally never has to build a full-history
// report (the root cause of the bills sync hang). An empty/`<LINEERROR>`
// response is NOT an error — it just means "no bill data in this window" — so
// it is returned as zero records instead of being retried/raised.
async function fetchBillsStream(xmlData, { entity = "bills", timeout = TALLY_TIMEOUT_MS } = {}) {
  const t0 = Date.now();
  const retries = 3;
  let lastErr = null;

  for (let attempt = 1; attempt <= retries; attempt++) {
    const heartbeat = setInterval(() => {
      const elapsed = formatDuration(Date.now() - t0);
      devLog(`[INFO] [${entity}] FETCH: still streaming from Tally... ${elapsed} elapsed (attempt ${attempt}/${retries})`);
      const entry = {
        ts: new Date().toISOString(),
        level: "info",
        msg: `[${entity}] FETCH: still streaming from Tally... ${elapsed} elapsed`,
        syncId: SYNC_RUN_ID
      };
      writeJsonLine(logFilePath("sync"), entry);
    }, 5000);
    if (heartbeat.unref) heartbeat.unref();

    try {
      const res = await axios.post(TALLY_URL, xmlData, {
        headers: { "Content-Type": "text/xml" },
        timeout,
        responseType: "stream"
      });

      const { parser, result } = saxBillsStream();
      let bytes = 0;

      await new Promise((resolve, reject) => {
        res.data.on("data", (chunk) => {
          res.data.pause();
          bytes += chunk.length;
          try {
            parser.write(chunk);
          } catch (e) {
            if (!res.data.destroyed) res.data.destroy();
            reject(e);
            return;
          }
          if (!res.data.destroyed) res.data.resume();
        });
        res.data.on("end", () => {
          try {
            parser.close();
            resolve();
          } catch (e) {
            reject(e);
          }
        });
        res.data.on("error", (err) => reject(err));
      });

      clearInterval(heartbeat);

      if (parser.seenLineError) {
        structuredLog("warn", `[${entity}] FETCH: Tally returned LINEERROR for window (no bill data)`, {
          duration: formatDuration(Date.now() - t0),
          size: fmtBytes(bytes)
        });
        return { records: [], bytes, lineError: true };
      }

      const records = [];
      for (let i = 0; i < result.fixed.length; i++) {
        records.push({
          fixed: result.fixed[i],
          cl: result.cl[i],
          due: result.due[i],
          over: result.over[i]
        });
      }

      structuredLog("info", `[${entity}] FETCH: response received`, {
        duration: formatDuration(Date.now() - t0),
        size: fmtBytes(bytes),
        records: records.length,
        attempt
      });
      return { records, bytes, lineError: false };
    } catch (err) {
      clearInterval(heartbeat);
      lastErr = err;
      structuredLog("warn", `[${entity}] FETCH: stream failed (attempt ${attempt}/${retries})`, {
        error: err.message
      });
      if (attempt < retries) await sleep(2000);
    }
  }
  throw lastErr;
}

// Streams Tally's XML response through a SAX parser, emitting one VOUCHER at a
// time. When `onBatch` is provided, vouchers are buffered into small batches and
// `onBatch(items)` is awaited (with backpressure) so the caller can persist each
// batch to the DB without ever holding a whole window in memory. Memory stays
// flat no matter the response size.
async function fetchVouchersStream(xmlData, onVoucher, { batchSize = 500, onBatch = null } = {}) {
  const t0 = Date.now();
  const retries = 3;
  const timeout = TALLY_TIMEOUT_MS;
  let lastErr = null;

  for (let attempt = 1; attempt <= retries; attempt++) {
    const heartbeat = setInterval(() => {
      const elapsed = formatDuration(Date.now() - t0);
      devLog(`[INFO] [vouchers] FETCH: still streaming from Tally... ${elapsed} elapsed (attempt ${attempt}/${retries})`);
      const entry = {
        ts: new Date().toISOString(),
        level: "info",
        msg: `[vouchers] FETCH: still streaming from Tally... ${elapsed} elapsed`,
        syncId: SYNC_RUN_ID
      };
      writeJsonLine(logFilePath("sync"), entry);
    }, 5000);
    if (heartbeat.unref) heartbeat.unref();

    try {
      const res = await axios.post(TALLY_URL, xmlData, {
        headers: { "Content-Type": "text/xml" },
        timeout,
        responseType: "stream"
      });

      const parser = saxVoucherStream(onVoucher);
      let bytes = 0;
      let pending = [];
      let pendingCount = 0;
      let flushing = false;

      // Drain the buffered vouchers into the caller-provided batch handler.
      // `await onBatch(...)` runs while the HTTP stream is paused (backpressure),
      // so memory never grows beyond ~batchSize vouchers.
      async function drain() {
        while (pending.length) {
          const items = pending;
          pending = [];
          pendingCount = 0;
          if (onVoucher) {
            for (const v of items) onVoucher(v);
          }
          if (onBatch) {
            await onBatch(items);
          }
        }
      }

      await new Promise((resolve, reject) => {
        res.data.on("data", (chunk) => {
          res.data.pause();
          bytes += chunk.length;
          try {
            parser.write(chunk);
            for (const v of parser.consumeDrained()) {
              pending.push(v);
              pendingCount++;
            }
            if (pendingCount >= batchSize && !flushing) {
              flushing = true;
              drain()
                .catch((e) => {
                  if (!res.data.destroyed) res.data.destroy();
                  reject(e);
                })
                .finally(() => {
                  flushing = false;
                  if (!res.data.destroyed) res.data.resume();
                });
            } else if (!res.data.destroyed) {
              res.data.resume();
            }
          } catch (e) {
            if (!res.data.destroyed) res.data.destroy();
            reject(e);
          }
        });
        res.data.on("end", async () => {
          try {
            parser.close();
            await drain();
            resolve();
          } catch (e) {
            reject(e);
          }
        });
        res.data.on("error", (err) => reject(err));
      });

      clearInterval(heartbeat);
      if (parser.seenLineError) {
        throw new Error("Tally returned LINEERROR (report request failed)");
      }
      structuredLog("info", "[vouchers] FETCH: response received", {
        duration: formatDuration(Date.now() - t0),
        size: fmtBytes(bytes),
        attempt
      });
      return { bytes };
    } catch (err) {
      clearInterval(heartbeat);
      lastErr = err;
      structuredLog("warn", `[vouchers] FETCH: stream failed (attempt ${attempt}/${retries})`, {
        error: err.message
      });
      if (attempt < retries) await sleep(2000);
    }
  }
  throw lastErr;
}

// ============================================================
//  SAFE BATCH WRITES (per-record isolation + retries)
// ============================================================
function buildBatchValues(rows, columns) {
  const placeholders = rows
    .map((_, rIdx) => `(${columns.map((_, cIdx) => `$${rIdx * columns.length + cIdx + 1}`).join(",")})`)
    .join(",");
  const values = rows.flatMap((r) =>
    columns.map((c) => (r[c] === undefined || r[c] === "" ? null : r[c]))
  );
  return { placeholders, values };
}

function buildUpsertStatement({ table, columns, rows, conflict, updateColumns }) {
  const { placeholders, values } = buildBatchValues(rows, columns);
  const conflictTarget = Array.isArray(conflict) ? `(${conflict.join(", ")})` : conflict;
  let sql;
  if (updateColumns && updateColumns.length) {
    const setClause = updateColumns.map((c) => `"${c}" = EXCLUDED."${c}"`).join(", ");
    // RETURNING (xmax = 0) is a read-only marker that lets us distinguish rows
    // actually INSERTED by this statement (xmax = 0) from rows that already
    // existed and were only UPDATED (xmax <> 0). No database behavior change.
    sql = `INSERT INTO "${table}" (${columns.map((c) => `"${c}"`).join(", ")})
           VALUES ${placeholders}
           ON CONFLICT ${conflictTarget} DO UPDATE SET ${setClause}
           RETURNING (xmax = 0) AS "_is_insert"`;
  } else {
    sql = `INSERT INTO "${table}" (${columns.map((c) => `"${c}"`).join(", ")})
           VALUES ${placeholders}
           ON CONFLICT ${conflictTarget} DO NOTHING`;
  }
  return { sql, values };
}

function buildInsertStatement({ table, columns, rows }) {
  const { placeholders, values } = buildBatchValues(rows, columns);
  const sql = `INSERT INTO "${table}" (${columns.map((c) => `"${c}"`).join(", ")})
               VALUES ${placeholders}`;
  return { sql, values };
}

let _savepointCounter = 0;

function isTransientDbError(err) {
  if (!err) return false;
  const code = String(err.code || "");
  if (/^(08|40|57)/.test(code)) return true;
  const msg = `${err.message || ""} ${err.code || ""}`;
  return /(ECONNRESET|ETIMEDOUT|EPIPE|EHOSTUNREACH|ENETUNREACH|connection (terminated|closed|reset)|socket hang up|timeout expired)/i.test(msg);
}

async function runSqlIsolated(db, inTx, sql, values) {
  if (inTx) {
    const sp = `sav_${_savepointCounter++}`;
    await db.query(`SAVEPOINT ${sp}`);
    try {
      const qres = await db.query(sql, values);
      await db.query(`RELEASE SAVEPOINT ${sp}`);
      return { ok: true, rowCount: qres.rowCount, rows: qres.rows };
    } catch (err) {
      try {
        await db.query(`ROLLBACK TO SAVEPOINT ${sp}`);
      } catch (rollbackErr) {
        throw err;
      }
      return { ok: false, error: err };
    }
  }
  try {
    const qres = await db.query(sql, values);
    return { ok: true, rowCount: qres.rowCount, rows: qres.rows };
  } catch (err) {
    return { ok: false, error: err };
  }
}

async function runSqlWithTransientRetry({ db, inTx, sql, values, entity, attempts = 3 }) {
  let lastErr;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    const res = await runSqlIsolated(db, inTx, sql, values);
    if (res.ok) return res;
    lastErr = res.error;
    if (!isTransientDbError(lastErr)) return res;
    if (attempt < attempts) {
      structuredLog("warn", `[${entity}] Transient DB error, retrying (${attempt}/${attempts})`, {
        error: lastErr.message,
        code: lastErr.code
      });
      await sleep(1000 * attempt);
    }
  }
  return { ok: false, error: lastErr };
}

// Counts how many rows of a successful batch were genuinely NEWLY INSERTED
// (vs already existing and merely updated). Uses the RETURNING marker for
// DO UPDATE upserts and rowCount for DO NOTHING / plain inserts. Purely
// in-memory bookkeeping — no extra queries, no database behavior change.
function countInsertedRows(res, rows) {
  if (res && Array.isArray(res.rows) && res.rows.length > 0 && res.rows[0]["_is_insert"] !== undefined) {
    return res.rows.filter((r) => r["_is_insert"] === true).length;
  }
  if (res && typeof res.rowCount === "number") return res.rowCount;
  return rows.length;
}

async function oneBatchSafe({ db, inTx, buildStatement, table, columns, rows, conflict, updateColumns, entity, idFn, depth = 0 }) {
  const { sql, values } = buildStatement({ table, columns, rows, conflict, updateColumns });
  const res = await runSqlWithTransientRetry({ db, inTx, sql, values, entity });
  if (res.ok) return { synced: rows.length, failed: [], inserted: countInsertedRows(res, rows) };

  if (rows.length === 1) {
    const record = rows[0];
    const identifier = idFn ? idFn(record) : "<unknown>";
    logFailure(entity, identifier, res.error, { table, record: summarizeRecord(record) });
    return { synced: 0, failed: [{ identifier, error: res.error }], inserted: 0 };
  }

  const mid = Math.floor(rows.length / 2);
  const left = await oneBatchSafe({ db, inTx, buildStatement, table, columns, rows: rows.slice(0, mid), conflict, updateColumns, entity, idFn, depth: depth + 1 });
  const right = await oneBatchSafe({ db, inTx, buildStatement, table, columns, rows: rows.slice(mid), conflict, updateColumns, entity, idFn, depth: depth + 1 });
  return { synced: left.synced + right.synced, failed: [...left.failed, ...right.failed], inserted: left.inserted + right.inserted };
}

async function writeRowsSafe({ db = pool, inTx = false, table, columns, rows, conflict, updateColumns, batchSize = 500, entity = table, idFn, onBatch }) {
  if (!rows || !rows.length) return { synced: 0, failed: [], total: 0, inserted: 0 };
  const useUpsert = !!(conflict && conflict.length);
  const buildStatement = useUpsert ? buildUpsertStatement : buildInsertStatement;
  const total = rows.length;
  let synced = 0;
  let inserted = 0;
  const failed = [];

  for (let i = 0; i < total; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    const batchStart = i + 1;
    const batchEnd = Math.min(i + batchSize, total);
    const t0 = Date.now();

    const r = await oneBatchSafe({ db, inTx, buildStatement, table, columns, rows: batch, conflict, updateColumns, entity, idFn });
    synced += r.synced;
    inserted += r.inserted || 0;
    failed.push(...r.failed);

    structuredLog("info", `[${entity}] SAVE progress: ${synced}/${total} records synced`, {
      saved: synced,
      newRecords: inserted,
      failed: failed.length,
      remaining: total - synced - failed.length,
      batch: `${batchStart}-${batchEnd}`,
      batchMs: Date.now() - t0
    });

    if (onBatch) onBatch({ synced, failed: failed.length, batchEnd, total });
  }

  return { synced, failed, total, inserted };
}

// ============================================================
//  DATABASE INIT (auto-create tables)
// ============================================================
const SCHEMA = `
CREATE TABLE IF NOT EXISTS companies (
  company_guid  TEXT PRIMARY KEY,
  name          TEXT NOT NULL,
  created_at    TIMESTAMPTZ DEFAULT NOW(),
  updated_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ledgers (
  company_guid    TEXT NOT NULL,
  ledger_guid     TEXT NOT NULL,
  name            TEXT NOT NULL,
  parent_group    TEXT,
  opening_balance NUMERIC DEFAULT 0,
  closing_balance NUMERIC DEFAULT 0,
  type            TEXT,
  phone           TEXT,
  email           TEXT,
  pan             TEXT,
  updated_at      TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (company_guid, ledger_guid)
);

CREATE TABLE IF NOT EXISTS vouchers (
  company_guid   TEXT NOT NULL,
  voucher_guid   TEXT NOT NULL,
  voucher_date   DATE,
  voucher_type   TEXT,
  reference_no   TEXT,
  net_amount     NUMERIC DEFAULT 0,
  is_cancelled   BOOLEAN DEFAULT FALSE,
  updated_at     TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (company_guid, voucher_guid)
);
DO $$ BEGIN
  ALTER TABLE vouchers ADD COLUMN is_cancelled BOOLEAN DEFAULT FALSE;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS invoices (
  company_guid   TEXT NOT NULL,
  invoice_guid   TEXT NOT NULL,
  invoice_no     TEXT,
  invoice_date   DATE,
  invoice_type   TEXT,
  party_name     TEXT,
  total_amount   NUMERIC DEFAULT 0,
  updated_at     TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (company_guid, invoice_guid)
);

CREATE TABLE IF NOT EXISTS orders (
  company_guid   TEXT NOT NULL,
  order_guid     TEXT NOT NULL,
  order_no       TEXT,
  order_date     DATE,
  party_name     TEXT,
  total_amount   NUMERIC DEFAULT 0,
  type           TEXT,
  updated_at     TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (company_guid, order_guid)
);

CREATE TABLE IF NOT EXISTS bills (
  id             BIGSERIAL PRIMARY KEY,
  company_guid   TEXT NOT NULL,
  ledger_name    TEXT,
  bill_name      TEXT,
  bill_date      DATE,
  bill_amount    NUMERIC DEFAULT 0,
  pending_amount NUMERIC DEFAULT 0,
  due_date       DATE,
  overdue_days   INT DEFAULT 0,
  bill_type      TEXT,
  source_hash    TEXT NOT NULL,
  UNIQUE (company_guid, source_hash)
);

CREATE TABLE IF NOT EXISTS inventory_items (
  company_guid   TEXT NOT NULL,
  item_guid      TEXT NOT NULL,
  name           TEXT NOT NULL,
  item_group     TEXT,
  unit           TEXT,
  opening_qty    NUMERIC DEFAULT 0,
  opening_value  NUMERIC DEFAULT 0,
  closing_qty    NUMERIC DEFAULT 0,
  closing_value  NUMERIC DEFAULT 0,
  updated_at     TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (company_guid, item_guid)
);

CREATE TABLE IF NOT EXISTS sync_log (
  id             BIGSERIAL PRIMARY KEY,
  company_guid   TEXT,
  company_name   TEXT,
  entity         TEXT,
  records        INT,
  synced_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Tracks incremental sync state per entity per company.
-- Replaces the fragile file-based checkpoint for drift detection
-- and "modified since" awareness.
CREATE TABLE IF NOT EXISTS sync_state (
  company_guid   TEXT NOT NULL,
  entity         TEXT NOT NULL,
  last_sync_at   TIMESTAMPTZ,
  last_window    TEXT,
  record_count   INT DEFAULT 0,
  checksum       TEXT,
  meta           JSONB DEFAULT '{}',
  PRIMARY KEY (company_guid, entity)
);
`;

// Add raw JSON payload column to every table (works for existing DBs too)
const PAYLOAD_COLUMNS = `
ALTER TABLE companies ADD COLUMN IF NOT EXISTS payload JSONB;
ALTER TABLE ledgers ADD COLUMN IF NOT EXISTS payload JSONB;
ALTER TABLE vouchers ADD COLUMN IF NOT EXISTS payload JSONB;
ALTER TABLE invoices ADD COLUMN IF NOT EXISTS payload JSONB;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS payload JSONB;
ALTER TABLE bills ADD COLUMN IF NOT EXISTS payload JSONB;
ALTER TABLE inventory_items ADD COLUMN IF NOT EXISTS payload JSONB;
`;

// Obsolete tables that are no longer produced by the agent. Dropped only after
// every code path that referenced them has been removed.
const DROP_OBSOLETE_TABLES = `
DROP TABLE IF EXISTS voucher_entries;
DROP TABLE IF EXISTS inventory_movements;
DROP TABLE IF EXISTS profit_loss;
`;

async function initDb() {
  const client = await pool.connect();
  try {
    await client.query(SCHEMA);
    await client.query(PAYLOAD_COLUMNS);
    await client.query(DROP_OBSOLETE_TABLES);
    console.log("✅ Database tables ready");
  } finally {
    client.release();
  }
}

// Generic upsert (duplicate-free) with batching to prevent Postgres parameter overflow
async function upsertRows({ table, columns, rows, conflict, updateColumns, batchSize = 500, db = null }) {
  if (!rows || !rows.length) return 0;

  const q = db || pool;
  let totalSynced = 0;

  for (let i = 0; i < rows.length; i += batchSize) {
    const batch = rows.slice(i, i + batchSize);
    const { sql, values } = buildUpsertStatement({
      table,
      columns,
      rows: batch,
      conflict,
      updateColumns
    });
    await q.query(sql, values);
    totalSynced += batch.length;
  }

  return totalSynced;
}

// Delete records for a company that no longer exist in Tally (uses single ANY array parameter)
async function cleanupByGuids(table, companyGuid, guidColumn, guids, db = null) {
  if (!guids || !guids.length) return;
  const q = db || pool;
  const sql = `DELETE FROM "${table}" WHERE company_guid = $1 AND NOT (${guidColumn} = ANY($2::text[]))`;
  const res = await q.query(sql, [companyGuid, guids]);
  devLog(`🧹 Cleanup ${table}: removed ${res.rowCount} missing records`);
}

function logSync(entity, records) {
  return pool.query(
    "INSERT INTO sync_log (company_guid, company_name, entity, records) VALUES ($1, $2, $3, $4)",
    [ACTIVE_COMPANY_GUID, ACTIVE_COMPANY_NAME, entity, records]
  );
}

// ============================================================
//  1. DETECT + CREATE COMPANIES
// ============================================================
const SHORT_MONTHS = {
  jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6,
  jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12
};

// Tally company names look like "Acme Ltd - (From 1-Apr-2016)".
function parseStartingFromName(name) {
  const m = /\(From\s+(\d{1,2})-([A-Za-z]+)-(\d{4})\)/i.exec(name || "");
  if (!m) return null;
  const mon = SHORT_MONTHS[(m[2] || "").toLowerCase().slice(0, 3)];
  if (!mon) return null;
  const day = parseInt(m[1], 10);
  const year = parseInt(m[3], 10);
  if (year < 1900 || year > 3000) return null;
  return `${year}-${String(mon).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
}

// Normalizes Tally date strings ("01-04-2016", "1-Apr-2016", "20160401") to ISO.
function normalizeTallyDate(raw) {
  if (!raw) return null;
  const s = String(raw).trim();
  let m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s);
  if (m) return s;
  m = /^(\d{8})$/.exec(s);
  if (m) return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`;
  m = /^(\d{1,2})-(\d{1,2})-(\d{4})$/.exec(s);
  if (m) {
    const day = parseInt(m[1], 10);
    const mon = parseInt(m[2], 10);
    if (mon >= 1 && mon <= 12 && day >= 1 && day <= 31) {
      return `${parseInt(m[3], 10)}-${String(mon).padStart(2, "0")}-${String(day).padStart(2, "0")}`;
    }
  }
  m = /^(\d{1,2})-([A-Za-z]+)-(\d{4})$/.exec(s);
  if (m) {
    const mon = SHORT_MONTHS[(m[2] || "").toLowerCase().slice(0, 3)];
    if (mon) {
      return `${parseInt(m[3], 10)}-${String(mon).padStart(2, "0")}-${String(parseInt(m[1], 10)).padStart(2, "0")}`;
    }
  }
  return null;
}

// Best-effort fetch of each company's "Starting From" date via a FIELD-based
// collection. If Tally rejects the request it returns a LINEERROR (never hangs);
// we simply fall back to name parsing / env SYNC_START_YEAR.
async function fetchCompanyStartDates() {
  const xmlRequest = `
<ENVELOPE>
 <HEADER>
  <VERSION>1</VERSION>
  <TALLYREQUEST>Export Data</TALLYREQUEST>
  <TYPE>Collection</TYPE>
  <ID>Company Starting Dates</ID>
 </HEADER>
 <BODY>
  <DESC>
   <STATICVARIABLES>
    <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
   </STATICVARIABLES>
    <TDL>
     <TDLMESSAGE>
      <COLLECTION NAME="Company Starting Dates">
       <TYPE>Company</TYPE>
       <NATIVEMETHOD>Name</NATIVEMETHOD>
       <NATIVEMETHOD>GUID</NATIVEMETHOD>
       <NATIVEMETHOD>StartingFrom</NATIVEMETHOD>
      </COLLECTION>
     </TDLMESSAGE>
    </TDL>
  </DESC>
 </BODY>
</ENVELOPE>
`;

  try {
    const response = await fetchTallyXml(xmlRequest, { entity: "company-dates" });
    if (!response.data || response.data.includes("<LINEERROR>")) return {};
    const jsonData = await parseStringPromise(response.data);
    const companies =
      jsonData?.ENVELOPE?.BODY?.[0]?.DATA?.[0]?.COLLECTION?.[0]?.COMPANY || [];
    const map = {};
    for (const c of companies) {
      const guid = typeof c.GUID?.[0] === "string" ? c.GUID[0] : c.GUID?.[0]?._;
      if (!guid) continue;
      let raw = c.STARTINGFROM;
      if (Array.isArray(raw)) raw = raw[0];
      if (raw && typeof raw === "object") raw = raw._;
      map[guid] = raw ? normalizeTallyDate(String(raw)) : null;
    }
    structuredLog("info", `[companies] Starting From dates fetched: ${Object.entries(map).filter(([, v]) => v).length}/${Object.keys(map).length} companies`);
    return map;
  } catch (err) {
    structuredLog("warn", "[companies] Could not fetch Starting From dates", {
      error: err.message
    });
    return {};
  }
}

async function detectCompanies() {
  const t0 = Date.now();

  const xmlRequest = `
<ENVELOPE>
 <HEADER>
  <VERSION>1</VERSION>
  <TALLYREQUEST>Export Data</TALLYREQUEST>
  <TYPE>Collection</TYPE>
  <ID>Company Collection</ID>
 </HEADER>
 <BODY>
  <DESC>
   <STATICVARIABLES>
    <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
   </STATICVARIABLES>
   <TDL>
    <TDLMESSAGE>
     <COLLECTION NAME="Company Collection">
      <TYPE>Company</TYPE>
      <FETCH>NAME,GUID</FETCH>
     </COLLECTION>
    </TDLMESSAGE>
   </TDL>
  </DESC>
 </BODY>
</ENVELOPE>
`;

  const response = await fetchTallyXml(xmlRequest, { entity: "companies" });

  const jsonData = await parseStringPromise(response.data);

  const companies =
    jsonData?.ENVELOPE?.BODY?.[0]?.DATA?.[0]?.COLLECTION?.[0]?.COMPANY || [];

  const companyList = [];
  let skipped = 0;

  for (const c of companies) {
    const company_guid =
      typeof c.GUID?.[0] === "string" ? c.GUID[0] : c.GUID?.[0]?._;
    const name =
      typeof c.NAME?.[0] === "string" ? c.NAME[0] : c.NAME?.[0]?._;

    if (!company_guid || !name) {
      skipped++;
      continue;
    }

    const startingFrom = parseStartingFromName(name);
    companyList.push({ company_guid, name, startingFrom, payload: c });
  }

  structuredLog("info", `[companies] VALIDATE: ${companies.length} fetched, ${skipped} skipped, ${companyList.length} valid`);

  // Best-effort: enrich with the real "Starting From" date reported by Tally
  // (companies without a "(From ...)" suffix in their name rely on this).
  const startDates = await fetchCompanyStartDates();
  for (const c of companyList) {
    if (!c.startingFrom && startDates[c.company_guid]) {
      c.startingFrom = startDates[c.company_guid];
    }
  }
  const withStartDate = companyList.filter((c) => c.startingFrom).length;
  structuredLog("info", `[companies] starting dates resolved for ${withStartDate}/${companyList.length} companies`);

  // 🔥 Auto-create companies in DB (duplicate-free upsert)
  const res = await writeRowsSafe({
    table: "companies",
    columns: ["company_guid", "name", "payload"],
    rows: companyList,
    conflict: ["company_guid"],
    updateColumns: ["name", "payload"],
    entity: "companies",
    idFn: (r) => r.company_guid
  });

  structuredLog("info", `[companies] COMPLETE: fetched=${companies.length} saved=${res.synced} failed=${res.failed.length} skipped=${skipped} remaining=0`, {
    duration: formatDuration(Date.now() - t0)
  });

  console.log(
    "🏢 Companies detected from Tally:",
    companyList.map((c) => c.name).join(", ")
  );

  return companyList;
}

// ============================================================
//  2. SYNC LEDGERS
// ============================================================
async function syncLedgers(company) {
  const t0 = Date.now();
  progressStart("Fetching Ledgers");

  const xmlRequest = `
<ENVELOPE>
 <HEADER>
  <VERSION>1</VERSION>
  <TALLYREQUEST>Export Data</TALLYREQUEST>
  <TYPE>Collection</TYPE>
  <ID>Ledger Collection</ID>
 </HEADER>
 <BODY>
  <DESC>
<STATICVARIABLES>
  <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
  <SVCURRENTCOMPANY>${company.name}</SVCURRENTCOMPANY>
</STATICVARIABLES>

   <TDL>
    <TDLMESSAGE>
     <COLLECTION NAME="Ledger Collection">
      <TYPE>Ledger</TYPE>
<FETCH>
  NAME,
  GUID,
  PARENT,
  ADDRESS,
  ADDRESS.LIST,
  MAILINGDETAILS.LIST,
  OPENINGBALANCE,
  CLOSINGBALANCE,
  ISBILLWISEON,
  LEDGERPHONE,
  EMAIL,
  INCOMETAXNUMBER
</FETCH>

     </COLLECTION>
    </TDLMESSAGE>
   </TDL>
  </DESC>
 </BODY>
</ENVELOPE>
`;

  const res = await fetchTallyXml(xmlRequest, { entity: "ledgers" });

  const parser = new xml2js.Parser({ explicitArray: false });
  const parsed = await parser.parseStringPromise(res.data);

  const ledgers = parsed?.ENVELOPE?.BODY?.DATA?.COLLECTION?.LEDGER || [];
  const ledgerArray = Array.isArray(ledgers) ? ledgers : [ledgers];

  devLog("Ledgers from Tally:", ledgerArray.length);
  progressUpdate(ledgerArray.length);

  const ledgerRows = [];
  const ledgerGuids = [];
  let skipped = 0;

  for (const l of ledgerArray) {
    let mailingAddressText = null;
    const mailingRaw = l["MAILINGDETAILS.LIST"];
    const mailingArr = Array.isArray(mailingRaw)
      ? mailingRaw
      : mailingRaw
      ? [mailingRaw]
      : [];

    for (const m of mailingArr) {
      if (m?.["ADDRESS.LIST"]?.ADDRESS) {
        mailingAddressText = extractAddressValue(m["ADDRESS.LIST"].ADDRESS);
        break;
      }
    }

    const ledgerName =
      l.$?.NAME ||
      l.NAME ||
      (typeof l.PARENT === "string" ? l.PARENT : null);

    if (!ledgerName) {
      devLog("⚠️ Skipping ledger without name");
      skipped++;
      continue;
    }

    const rawPhone = l.LEDGERPHONE?._ || l.LEDGERPHONE || null;
    const rawEmail = l.EMAIL?._ || l.EMAIL || null;
    const addressText = normalizeText(l.ADDRESS);
    let addressListText = null;
    if (l["ADDRESS.LIST"]?.ADDRESS) {
      addressListText = extractAddressValue(l["ADDRESS.LIST"].ADDRESS);
    }

    const searchBlob = [
      rawPhone,
      rawEmail,
      addressText,
      addressListText,
      mailingAddressText,
      l.NAME,
      l.PARENT
    ]
      .filter(Boolean)
      .join(" ");

    const phone = extractPhone(searchBlob);
    const email = extractEmail(searchBlob);

    const pan =
      typeof l.INCOMETAXNUMBER === "string"
        ? l.INCOMETAXNUMBER
        : l.INCOMETAXNUMBER?._ || null;

    const ledgerGuid =
      typeof l.GUID === "string" ? l.GUID : l.GUID?._ || null;

    if (!ledgerGuid || !ledgerName) {
      devLog("⏭️ Skipping invalid ledger:", ledgerName);
      skipped++;
      continue;
    }

    ledgerGuids.push(ledgerGuid);

    ledgerRows.push({
      company_guid: company.company_guid,
      ledger_guid: ledgerGuid,
      name: ledgerName,
      parent_group: typeof l.PARENT === "string" ? l.PARENT : l.PARENT?._,
      opening_balance: parseTallyNumber(l.OPENINGBALANCE?._ || "0"),
      closing_balance: parseTallyNumber(l.CLOSINGBALANCE?._ || "0"),
      type: l.ISBILLWISEON === "Yes" ? "Party" : "General",
      phone,
      email,
      pan,
      payload: l
    });
  }

  structuredLog("info", `[ledgers] VALIDATE: ${ledgerArray.length} fetched, ${skipped} skipped, ${ledgerRows.length} valid`);

  const result = await writeRowsSafe({
    table: "ledgers",
    columns: [
      "company_guid",
      "ledger_guid",
      "name",
      "parent_group",
      "opening_balance",
      "closing_balance",
      "type",
      "phone",
      "email",
      "pan",
      "payload"
    ],
    rows: ledgerRows,
    conflict: ["company_guid", "ledger_guid"],
    updateColumns: [
      "name",
      "parent_group",
      "opening_balance",
      "closing_balance",
      "type",
      "phone",
      "email",
      "pan",
      "payload"
    ],
    entity: "ledgers",
    idFn: (r) => r.ledger_guid
  });

  await cleanupByGuids("ledgers", company.company_guid, "ledger_guid", ledgerGuids);
  await logSync("ledgers", result.synced);
  progressEnd(ledgerRows.length, "Ledgers");

  const ledgerChecksum = computeGuidChecksum(ledgerGuids);
  const prevLedgerState = await getSyncState(ACTIVE_COMPANY_GUID, "ledgers");
  const ledgerChanged = prevLedgerState ? prevLedgerState.checksum !== ledgerChecksum : true;

  recordEntityStats("ledgers", {
    fetched: ledgerArray.length,
    saved: result.synced,
    failed: result.failed.length,
    skipped,
    newCount: result.inserted
  });

  structuredLog("info", `[ledgers] COMPLETE: fetched=${ledgerArray.length} saved=${result.synced} new=${result.inserted} failed=${result.failed.length} skipped=${skipped} remaining=0 changed=${ledgerChanged ? "yes" : "no"}`, {
    duration: formatDuration(Date.now() - t0)
  });
  const ledgerDbCount = await getDbCount("ledgers", ACTIVE_COMPANY_GUID);
  devLog(`✅ Ledgers: ${result.synced} new (${ledgerDbCount} in DB)${ledgerChanged ? " ⚡ changes detected" : " (unchanged)"}`);

  try {
    await upsertSyncState(ACTIVE_COMPANY_GUID, "ledgers", {
      lastSyncAt: new Date().toISOString(),
      recordCount: ledgerArray.length,
      checksum: ledgerChecksum,
      meta: { changed: ledgerChanged }
    });
  } catch {}
}

// Helper: get DB count for any entity table
async function getDbCount(table, companyGuid, extraWhere = "", extraParams = []) {
  try {
    const where = extraWhere ? `AND ${extraWhere}` : "";
    const { rows } = await pool.query(
      `SELECT count(*)::int AS n FROM ${table} WHERE company_guid = $1 ${where}`,
      [companyGuid, ...extraParams]
    );
    return rows[0]?.n ?? 0;
  } catch { return 0; }
}

// ============================================================
//  3. SYNC VOUCHERS (+ INVOICES + ENTRIES)
// ============================================================
async function syncVouchers(company, opts = {}) {
  if (!ACTIVE_COMPANY_GUID) return;
  const t0 = Date.now();

  // Invoices are derived from the same "Voucher Register" response as vouchers,
  // so either selection triggers the (single) Tally request; only the selected
  // outputs are written to the database. Defaults keep old callers syncing both.
  const saveVouchers = opts.saveVouchers !== false;
  const saveInvoices = opts.saveInvoices !== false;

  // Live single-line progress for the shared Voucher Register fetch.
  const voucherProgressLabel =
    saveVouchers && saveInvoices
      ? "Fetching Vouchers & Invoices"
      : saveVouchers
      ? "Fetching Vouchers"
      : "Fetching Invoices";
  progressStart(voucherProgressLabel);

  // Smaller windows = smaller Tally responses = each window finishes fast and
  // checkpoints stay fine-grained on huge companies. Overridable via
  // VOUCHER_WINDOW_DAYS (e.g. 1 for very heavy companies).
  const VOUCHER_WINDOW_DAYS = Number(process.env.VOUCHER_WINDOW_DAYS) || 3;
  const syncStartYear = resolveSyncStartYear(company);
  const chunks = getVoucherWindows(new Date(syncStartYear, 3, 1), VOUCHER_WINDOW_DAYS);

  const checkpoint = getCompanyCheckpoint(ACTIVE_COMPANY_GUID).vouchers || {};
  let lastWindow = checkpoint.startYear === syncStartYear ? checkpoint.lastWindow || null : null;

  // Self-heal: if the DB holds no rows for the selected output (table cleared,
  // DB restored, etc.), the resume checkpoint is stale — it would skip every
  // window and leave the data permanently empty. Ignore the checkpoint and do
  // a full re-sync so the rows get fetched again.
  if (lastWindow) {
    try {
      const outputTable = saveVouchers ? "vouchers" : "invoices";
      const { rows } = await pool.query(
        `SELECT count(*)::int AS n FROM ${outputTable} WHERE company_guid = $1`,
        [ACTIVE_COMPANY_GUID]
      );
      if (!rows[0] || rows[0].n === 0) {
        structuredLog("warn", `[vouchers] DB has no ${outputTable} rows, ignoring checkpoint and re-syncing all windows`);
        lastWindow = null;
      }
    } catch (err) {
      structuredLog("warn", "[vouchers] could not verify output count, keeping checkpoint", {
        error: err.message
      });
    }
  }

  // ⚠️ BOUNDED REQUEST: "Voucher Register" REPORT, NOT a bare VoucherCollection.
  // A bare Voucher collection ignores SVFROMDATE/SVTODATE and returns the WHOLE
  // company (current financial year) in one shot — on large companies that makes
  // Tally Prime run out of memory ("memory excess violation") and hang. The
  // register report honours the date range, so each window stays small.
  // Cancelled vouchers are filtered client-side (ISDELETED / ISCANCELLED check).
  const xmlTemplate = `
<ENVELOPE>
 <HEADER>
  <TALLYREQUEST>Export Data</TALLYREQUEST>
 </HEADER>
 <BODY>
  <EXPORTDATA>
   <REQUESTDESC>
    <REPORTNAME>Voucher Register</REPORTNAME>
    <STATICVARIABLES>
     <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
     <SVCURRENTCOMPANY>${ACTIVE_COMPANY_NAME}</SVCURRENTCOMPANY>
     <SVFROMDATE>__FROM_DATE__</SVFROMDATE>
     <SVTODATE>__TO_DATE__</SVTODATE>
    </STATICVARIABLES>
   </REQUESTDESC>
  </EXPORTDATA>
 </BODY>
</ENVELOPE>
`;
  let skipped = 0;
  let cancelledCount = 0;
  let totalFetched = 0;
  const voucherTypeCounts = {};
  const cancelledTypeCounts = {};
  // Deduplicate: Voucher Register XML returns each voucher multiple times
  // (once per voucher-type grouping). Without dedup the same cancelled voucher
  // is counted N times, inflating the cancelled total.
  const countedGuids = new Set();
  const stats = {
    voucherSaved: 0,
    voucherFailed: 0,
    voucherInserted: 0,
    invoiceSaved: 0,
    invoiceFailed: 0,
    invoiceInserted: 0
  };

  function extractValue(val) {
    if (!val) return null;
    if (typeof val === "string") return val;
    if (typeof val === "object" && val._) return val._;
    return null;
  }

  function processVoucher(v, rows) {
    const voucherGuid = extractValue(v.GUID?.[0]);
    if (!voucherGuid) {
      skipped++;
      return;
    }

    // Deduplicate: Voucher Register XML repeats each voucher ~N times (once per
    // type grouping). We only count / process each GUID ONCE.
    if (countedGuids.has(voucherGuid)) return;
    countedGuids.add(voucherGuid);

    // Count cancelled / deleted vouchers separately — still save them with is_cancelled flag
    const isDeleted = v.ISDELETED ? v.ISDELETED[0] : null;
    const isCancelled = v.ISCANCELLED ? v.ISCANCELLED[0] : null;
    const voucherIsCancelled = isDeleted === "Yes" || isCancelled === "Yes";
    if (voucherIsCancelled) {
      cancelledCount++;
      const rawType = extractValue(v.VOUCHERTYPENAME?.[0] || v.VOUCHERTYPE?.[0]);
      const cTypeKey = (rawType || "").trim() || "Unknown";
      cancelledTypeCounts[cTypeKey] = (cancelledTypeCounts[cTypeKey] || 0) + 1;
    }

    rows.chunkGuids.push(voucherGuid);

    const rawDate = extractValue(v.DATE?.[0] || v.REFERENCEDATE?.[0]);
    const voucherDate = toISODate(rawDate);
    const voucherType = extractValue(v.VOUCHERTYPENAME?.[0] || v.VOUCHERTYPE?.[0]);
    const vTypeKey = (voucherType || "").trim() || "Unknown";
    if (!voucherIsCancelled) {
      voucherTypeCounts[vTypeKey] = (voucherTypeCounts[vTypeKey] || 0) + 1;
    }
    const referenceNo =
      extractValue(v.REFERENCE?.[0] || v.REFERENCENUMBER?.[0] || v.VOUCHERNUMBER?.[0]);

    const rawLedgerEntries = [
      ...(Array.isArray(v["ALLLEDGERENTRIES.LIST"])
        ? v["ALLLEDGERENTRIES.LIST"]
        : v["ALLLEDGERENTRIES.LIST"]
        ? [v["ALLLEDGERENTRIES.LIST"]]
        : []),
      ...(Array.isArray(v["LEDGERENTRIES.LIST"])
        ? v["LEDGERENTRIES.LIST"]
        : v["LEDGERENTRIES.LIST"]
        ? [v["LEDGERENTRIES.LIST"]]
        : [])
    ];

    const entryArray = Array.isArray(rawLedgerEntries)
      ? rawLedgerEntries.filter((e) => typeof e === "object")
      : typeof rawLedgerEntries === "object"
      ? [rawLedgerEntries]
      : [];

    const entries = [];

    for (const e of entryArray) {
      const ledgerName = extractValue(e.LEDGERNAME?.[0]);
      const amountRaw = extractValue(e.AMOUNT?.[0]);
      if (!ledgerName || !amountRaw) continue;

      const isDebit = extractValue(e.ISDEEMEDPOSITIVE?.[0]) === "No";

      entries.push({
        company_guid: ACTIVE_COMPANY_GUID,
        voucher_guid: voucherGuid,
        ledger_name: ledgerName,
        amount: Math.abs(Number(amountRaw)),
        is_debit: isDebit,
        payload: e
      });
    }

    if (entries.length === 0 && v.PARTYLEDGERNAME?.[0]) {
      entries.push({
        company_guid: ACTIVE_COMPANY_GUID,
        voucher_guid: voucherGuid,
        ledger_name: extractValue(v.PARTYLEDGERNAME?.[0]),
        amount: 0,
        is_debit: false,
        payload: v
      });
    }

    let debitTotal = 0;
    let creditTotal = 0;
    for (const entry of entries) {
      if (entry.is_debit) debitTotal += entry.amount;
      else creditTotal += entry.amount;
    }

    const netAmount = Math.max(debitTotal, creditTotal);

    if (saveVouchers) {
      rows.voucherRows.push({
        company_guid: ACTIVE_COMPANY_GUID,
        voucher_guid: voucherGuid,
        voucher_date: voucherDate,
        voucher_type: voucherType,
        reference_no: referenceNo,
        net_amount: netAmount,
        is_cancelled: voucherIsCancelled,
        payload: v
      });
    }

    const isPurchase =
      voucherType && voucherType.toLowerCase().includes("purchase");
    const isSales =
      voucherType &&
      (voucherType.toLowerCase().includes("sales") ||
        voucherType.toLowerCase().includes("invoice"));

    if (saveInvoices && !voucherIsCancelled && (isPurchase || isSales)) {
      rows.invoiceRows.push({
        company_guid: ACTIVE_COMPANY_GUID,
        invoice_guid: voucherGuid,
        invoice_no: extractValue(v.VOUCHERNUMBER?.[0]),
        invoice_date: voucherDate,
        invoice_type: isPurchase ? "Purchase" : "Sales",
        party_name: extractValue(v.PARTYLEDGERNAME?.[0]),
        total_amount: netAmount,
        payload: v
      });
    }
  }

  async function writeChunk(rows) {
    if (!rows.chunkGuids.length) return;
    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      const voucherRes = saveVouchers
        ? await writeRowsSafe({
            db: client,
            inTx: true,
            table: "vouchers",
            columns: [
              "company_guid",
              "voucher_guid",
              "voucher_date",
              "voucher_type",
              "reference_no",
              "net_amount",
              "is_cancelled",
              "payload"
            ],
            rows: rows.voucherRows,
            conflict: ["company_guid", "voucher_guid"],
            updateColumns: [
              "voucher_date",
              "voucher_type",
              "reference_no",
              "net_amount",
              "is_cancelled",
              "payload"
            ],
            entity: "vouchers",
            idFn: (r) => r.voucher_guid
          })
        : { synced: 0, failed: [] };

      const invoiceRes = saveInvoices
        ? await writeRowsSafe({
            db: client,
            inTx: true,
            table: "invoices",
            columns: [
              "company_guid",
              "invoice_guid",
              "invoice_no",
              "invoice_date",
              "invoice_type",
              "party_name",
              "total_amount",
              "payload"
            ],
            rows: rows.invoiceRows,
            conflict: ["company_guid", "invoice_guid"],
            updateColumns: [
              "invoice_no",
              "invoice_date",
              "invoice_type",
              "party_name",
              "total_amount",
              "payload"
            ],
            entity: "invoices",
            idFn: (r) => r.invoice_guid
          })
        : { synced: 0, failed: [] };

      await client.query("COMMIT");

      stats.voucherSaved += voucherRes.synced;
      stats.voucherFailed += voucherRes.failed.length;
      stats.voucherInserted += voucherRes.inserted || 0;
      stats.invoiceSaved += invoiceRes.synced;
      stats.invoiceFailed += invoiceRes.failed.length;
      stats.invoiceInserted += invoiceRes.inserted || 0;
    } catch (err) {
      await client.query("ROLLBACK");
      structuredLog("error", "[vouchers] Batch sync failed, transaction rolled back", {
        error: err.message
      });
      throw err;
    } finally {
      client.release();
    }
  }

  // Removes vouchers that Tally no longer reports for this window (bounded to the
  // window's own date range, so memory stays flat regardless of company size).
  // Only runs when the window produced at least one voucher, so an empty Tally
  // response (e.g. wrong active date) can never wipe existing data.
  async function cleanupWindow(fromDate, toDate, windowGuids) {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      let removed = [];
      if (saveVouchers) {
        const delRes = await client.query(
          `DELETE FROM vouchers
            WHERE company_guid = $1
              AND voucher_date >= $2::date
              AND voucher_date <= $3::date
              AND NOT (voucher_guid = ANY($4::text[]))
            RETURNING voucher_guid`,
          [ACTIVE_COMPANY_GUID, fromDate, toDate, windowGuids]
        );
        removed = delRes.rows.map((r) => r.voucher_guid);
      }
      if (removed.length && saveInvoices) {
        await client.query(
          "DELETE FROM invoices WHERE company_guid = $1 AND invoice_guid = ANY($2::text[])",
          [ACTIVE_COMPANY_GUID, removed]
        );
      }
      await client.query("COMMIT");
      structuredLog("info", `[vouchers] WINDOW CLEANUP: ${fromDate}-${toDate} removed ${removed.length} stale vouchers`);
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  }

  async function processDateRange(fromDate, toDate) {
    const xmlRequest = xmlTemplate
      .replace("__FROM_DATE__", fromDate)
      .replace("__TO_DATE__", toDate);

    const windowGuids = [];
    let fetchedInWindow = 0;

    const onBatch = async (items) => {
      const batch = { voucherRows: [], invoiceRows: [], chunkGuids: [] };
      for (const v of items) {
        fetchedInWindow++;
        totalFetched++;
        processVoucher(v, batch);
      }
      if (batch.chunkGuids.length) {
        await writeChunk(batch);
        windowGuids.push(...batch.chunkGuids);
      }
      // Live progress: unique vouchers successfully fetched + persisted so far
      progressUpdate(countedGuids.size);
    };

    const { bytes } = await fetchVouchersStream(xmlRequest, null, {
      batchSize: 500,
      onBatch
    });

    if (windowGuids.length) {
      try {
        await cleanupWindow(fromDate, toDate, windowGuids);
      } catch (cleanupErr) {
        structuredLog("warn", `[vouchers] WINDOW CLEANUP FAILED (non-fatal): ${fromDate}-${toDate}`, {
          error: cleanupErr.message
        });
      }
    }

    structuredLog("info", `[vouchers] WINDOW DONE: ${fromDate}-${toDate} fetched=${fetchedInWindow} valid=${windowGuids.length} skipped=${skipped} size=${fmtBytes(bytes)}`);

    setCompanyCheckpoint(ACTIVE_COMPANY_GUID, "vouchers", fromDate, syncStartYear);
  }

  // Incremental resume: skip already-synced windows that are fully in the past
  // AND older than the recent re-fetch period. Windows within RECENT_WINDOW_DAYS
  // of today are always re-fetched even if checkpointed, so edits/creations in
  // recent history are detected automatically.
  // RECENT_WINDOW_DAYS: how many days back to always re-fetch (default 30).
  const RECENT_WINDOW_DAYS = Number(process.env.RECENT_WINDOW_DAYS) || 30;
  const recentCutoff = new Date();
  recentCutoff.setDate(recentCutoff.getDate() - RECENT_WINDOW_DAYS);
  const recentCutoffYmd = fmtYMD(recentCutoff);

  const today = fmtYMD(new Date());
  let processed = 0;
  let resumed = 0;
  let reFetchedRecent = 0;
  for (let i = 0; i < chunks.length; i++) {
    const chunk = chunks[i];
    // Skip ONLY if: already checkpointed AND fully in the past AND older than recent cutoff
    if (lastWindow && chunk.label <= lastWindow && chunk.toDate < today && chunk.toDate < recentCutoffYmd) {
      resumed++;
      continue;
    }
    // Track re-fetches of previously-synced recent windows
    if (lastWindow && chunk.label <= lastWindow) {
      reFetchedRecent++;
    }
    structuredLog("info", `[vouchers] FETCH: window ${chunk.label} (${i + 1}/${chunks.length}) ${chunk.fromDate}-${chunk.toDate}`);
    try {
      await processDateRange(chunk.fromDate, chunk.toDate);
      processed++;
    } catch (windowErr) {
      structuredLog("warn", `[vouchers] WINDOW FAILED (non-fatal): ${chunk.fromDate}-${chunk.toDate}`, {
        error: windowErr.message
      });
      progressBreak();
      if (isDeveloperMode) {
        console.log(`⚠️  Window ${chunk.label} failed: ${windowErr.message} — continuing...`);
      } else {
        console.log(`⚠️ Unable to sync Vouchers for some periods.`);
        console.log(`   Please contact the administrator.`);
      }
    }
    if (i < chunks.length - 1) await sleep(800);
  }

  progressEnd(
    countedGuids.size,
    saveVouchers && saveInvoices
      ? "Vouchers & Invoices"
      : saveVouchers
      ? "Vouchers"
      : "Invoices"
  );

  structuredLog("info", `[vouchers] VALIDATE: ${totalFetched} fetched, ${skipped} skipped, ${processed} windows processed, ${resumed} windows resumed from checkpoint, ${reFetchedRecent} recent windows re-fetched`);

  // --- DB count for sync_state ---
  let dbVoucherCount = 0;
  try {
    const { rows } = await pool.query(
      `SELECT count(*)::int AS n FROM vouchers WHERE company_guid = $1`,
      [ACTIVE_COMPANY_GUID]
    );
    dbVoucherCount = rows[0]?.n ?? 0;
  } catch {}

  try {
    const { rows } = await pool.query(
      `SELECT
         (SELECT count(*)::int FROM vouchers WHERE company_guid = $1) AS vouchers,
         (SELECT count(*)::int FROM invoices WHERE company_guid = $1) AS invoices`,
      [ACTIVE_COMPANY_GUID]
    );
    const t = rows[0];
    devLog(
      `✅ Vouchers: ${stats.voucherSaved} new (${t.vouchers} in DB) | Invoices: ${stats.invoiceSaved} new (${t.invoices} in DB)`
    );
  } catch (e) {
    devLog(`✅ Vouchers: ${stats.voucherSaved} | Invoices: ${stats.invoiceSaved}`);
  }

  // Tally-style breakdown: Active / Cancelled / Total per voucher type
  const allTypes = new Set([
    ...Object.keys(voucherTypeCounts),
    ...Object.keys(cancelledTypeCounts)
  ]);

  // Store breakdown for the final summary (even if empty from checkpoint-resumed runs)
  const typeArr = [...allTypes].map((t) => ({
    type: t,
    active: voucherTypeCounts[t] || 0,
    cancelled: cancelledTypeCounts[t] || 0,
    total: (voucherTypeCounts[t] || 0) + (cancelledTypeCounts[t] || 0)
  }));
  typeArr.sort((a, b) => b.total - a.total);

  if (allTypes.size > 0 && isDeveloperMode) {
    console.log("");
    console.log("Voucher Type Breakdown (Active / Cancelled / Total):");
    let grandActive = 0, grandCancelled = 0, grandTotal = 0;
    for (const r of typeArr) {
      console.log(
        `  • ${r.type}: ${r.active.toLocaleString()} / ${r.cancelled.toLocaleString()} / ${r.total.toLocaleString()}`
      );
      grandActive += r.active;
      grandCancelled += r.cancelled;
      grandTotal += r.total;
    }
    console.log(
      `  ── TOTAL: ${grandActive.toLocaleString()} / ${grandCancelled.toLocaleString()} / ${grandTotal.toLocaleString()}`
    );
    console.log("");
  }

  // Store for final printSyncSummary
  if (typeArr.length > 0) {
    SYNC_SUMMARY.voucherTypeBreakdown = typeArr;
    if (CURRENT_COMPANY_STATS) CURRENT_COMPANY_STATS.voucherTypeBreakdown = typeArr;
  }
  if (saveVouchers) await logSync("vouchers", stats.voucherSaved);
  if (saveInvoices) await logSync("invoices", stats.invoiceSaved);

  if (saveVouchers) {
    recordEntityStats("vouchers", {
      fetched: totalFetched,
      saved: stats.voucherSaved,
      failed: stats.voucherFailed,
      skipped,
      newCount: stats.voucherInserted
    });
  }
  if (saveInvoices) {
    recordEntityStats("invoices", {
      fetched: totalFetched,
      saved: stats.invoiceSaved,
      failed: stats.invoiceFailed,
      skipped: 0,
      newCount: stats.invoiceInserted
    });
  }

  structuredLog("info", `[vouchers] COMPLETE: fetched=${totalFetched} saved=${stats.voucherSaved} new=${stats.voucherInserted} failed=${stats.voucherFailed} skipped=${skipped} remaining=0`, {
    duration: formatDuration(Date.now() - t0)
  });

  // Persist incremental sync state for next run
  try {
    const syncMeta = {
      windowsProcessed: processed,
      windowsResumed: resumed,
      windowsReFetchedRecent: reFetchedRecent,
      cancelledCount,
      recentWindowDays: RECENT_WINDOW_DAYS
    };
    // Store the full voucher type breakdown so it survives incremental runs
    // where no vouchers are fetched (all windows checkpointed). This lets the
    // summary always show the last-known breakdown with cancelled counts.
    if (typeArr.length > 0) {
      syncMeta.voucherTypeBreakdown = typeArr;
    }
    await upsertSyncState(ACTIVE_COMPANY_GUID, "vouchers", {
      lastSyncAt: new Date().toISOString(),
      lastWindow: chunks.length ? chunks[chunks.length - 1].fromDate : null,
      recordCount: dbVoucherCount,
      meta: syncMeta
    });
  } catch {}
}

// ============================================================
//  4. SYNC BILLS (Receivable + Payable) — windowed & streamed
// ============================================================
async function syncBills(company) {
  return syncBillsForType("RECEIVABLE", company);
}

async function syncBillsPayable(company) {
  return syncBillsForType("PAYABLE", company);
}

// Shared bills sync. Receivable/Payable differ only in the report name and the
// bill_type tag, so one function drives both. Live-verified against TallyPrime:
// the "Bills Receivable"/"Bills Payable" report returns ALL outstanding bills
// as of SVTODATE, and a single bounded request (company start -> today) returns
// the complete current snapshot in seconds on the large company. Sending the
// full-history dates (SVFROMDATE=19000101/SVTODATE=20991231) hangs Tally, so we
// only ever send real, in-range dates. Responses are streamed through a SAX
// parser (no DOM tree), rows are upserted, stale (settled) bills are removed
// with a full snapshot replace, and a checkpoint is saved so a failed fetch is
// retried next run.
async function syncBillsForType(billType, company) {
  if (!ACTIVE_COMPANY_GUID) return;
  const t0 = Date.now();

  const reportName = billType === "PAYABLE" ? "Bills Payable" : "Bills Receivable";
  const entity = billType === "PAYABLE" ? "bills_payable" : "bills_receivable";

  progressStart(`Fetching ${reportName}`);

  const syncStartYear = resolveSyncStartYear(company);
  let billsFrom = `${syncStartYear}-04-01`;
  if (company && company.startingFrom && /^\d{4}-\d{2}-\d{2}$/.test(company.startingFrom)) {
    if (company.startingFrom > billsFrom) billsFrom = company.startingFrom;
  }
  const billsToday = fmtYMD(new Date());
  const windows = [{
    fromDate: billsFrom,
    toDate: billsToday < billsFrom ? billsFrom : billsToday,
    label: billsToday
  }];

  const state = getBillSyncState(billType);
  const lastWindow = state.startYear === syncStartYear ? state.lastWindow || null : null;

  const xmlTemplate = `
<ENVELOPE>
 <HEADER>
  <TALLYREQUEST>Export Data</TALLYREQUEST>
 </HEADER>
 <BODY>
  <EXPORTDATA>
   <REQUESTDESC>
    <REPORTNAME>${reportName}</REPORTNAME>
    <STATICVARIABLES>
     <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
     <SVCURRENTCOMPANY>${ACTIVE_COMPANY_NAME}</SVCURRENTCOMPANY>
     <SVCOMPANY>${ACTIVE_COMPANY_NAME}</SVCOMPANY>
     <SVFROMDATE>__FROM_DATE__</SVFROMDATE>
     <SVTODATE>__TO_DATE__</SVTODATE>
    </STATICVARIABLES>
   </REQUESTDESC>
  </EXPORTDATA>
 </BODY>
</ENVELOPE>
`;

  const today = fmtYMD(new Date());
  let totalFetched = 0;
  let totalSaved = 0;
  let totalNew = 0;
  let totalFailed = 0;
  let skipped = 0;
  let processed = 0;
  let resumed = 0;

  for (let i = 0; i < windows.length; i++) {
    const window = windows[i];
    // Incremental resume: skip windows fully in the past that are already
    // checkpointed, but ALWAYS re-fetch the in-progress window (the one
    // containing "now") so bills added after the previous sync are picked up.
    if (lastWindow && window.label <= lastWindow && window.toDate < today) {
      resumed++;
      continue;
    }

    structuredLog("info", `[${entity}] FETCH: window ${window.label} (${i + 1}/${windows.length}) ${window.fromDate}-${window.toDate}`);

    const xmlRequest = xmlTemplate
      .replace("__FROM_DATE__", window.fromDate.replace(/-/g, ""))
      .replace("__TO_DATE__", window.toDate.replace(/-/g, ""));

    const { records, bytes, lineError } = await fetchBillsStream(xmlRequest, { entity });

    const rows = [];
    let windowSkipped = 0;
    for (const rec of records) {
      totalFetched++;
      const bill = rec.fixed;

      const ledgerName = normalizeText(bill.BILLPARTY);
      const billName = normalizeText(bill.BILLREF);
      const billDate = normalizeText(bill.BILLDATE);

      if (!ledgerName || !billName) {
        skipped++;
        windowSkipped++;
        continue;
      }

      const pendingAmt = Math.abs(parseTallyNumber(rec.cl || "0"));
      const dueDate = normalizeText(rec.due || null);
      const overdueDays = billType === "PAYABLE" ? 0 : parseTallyNumber(rec.over || "0");

      rows.push({
        company_guid: ACTIVE_COMPANY_GUID,
        ledger_name: ledgerName,
        bill_name: billName,
        bill_date: toISODate(billDate),
        bill_amount: pendingAmt,
        pending_amount: pendingAmt,
        due_date: toISODate(dueDate),
        overdue_days: overdueDays,
        bill_type: billType,
        source_hash: makeHash(
          ACTIVE_COMPANY_GUID,
          billName,
          billType,
          ledgerName,
          pendingAmt,
          dueDate
        ),
        payload: bill
      });
    }

    if (rows.length) {
      const result = await writeRowsSafe({
        table: "bills",
        columns: [
          "company_guid",
          "ledger_name",
          "bill_name",
          "bill_date",
          "bill_amount",
          "pending_amount",
          "due_date",
          "overdue_days",
          "bill_type",
          "source_hash",
          "payload"
        ],
        rows,
        conflict: ["company_guid", "source_hash"],
        updateColumns: [
          "ledger_name",
          "bill_date",
          "bill_amount",
          "pending_amount",
          "due_date",
          "overdue_days",
          "bill_type",
          "payload"
        ],
        entity,
        idFn: (r) => `${r.ledger_name}/${r.bill_name} (${r.bill_type})`
      });
      totalSaved += result.synced;
      totalNew += result.inserted || 0;
      totalFailed += result.failed.length;
    }

    // Full snapshot replace: remove bills Tally no longer reports for the
    // company (i.e. now-settled bills). The single window spans the company's
    // whole history, so this is a complete replace for the bill type. Skipped
    // when the fetch produced nothing (empty/LINEERROR response), so a silent
    // Tally failure can never wipe existing data.
    if (records.length && !lineError) {
      const delRes = await pool.query(
        `DELETE FROM bills
          WHERE company_guid = $1
            AND bill_type = $2
            AND NOT (source_hash = ANY($3::text[]))
          RETURNING source_hash`,
        [
          ACTIVE_COMPANY_GUID,
          billType,
          rows.map((r) => r.source_hash)
        ]
      );
      if (delRes.rows.length) {
        structuredLog("info", `[${entity}] CLEANUP: removed ${delRes.rows.length} stale bills no longer reported`);
      }
    }

    saveBillSyncState(billType, window.label, syncStartYear);

    structuredLog("info", `[${entity}] WINDOW DONE: ${window.fromDate}-${window.toDate} fetched=${records.length} valid=${rows.length} skipped=${windowSkipped} size=${fmtBytes(bytes)}`);
    progressUpdate(totalFetched);
    processed++;

    if (i < windows.length - 1) await sleep(800);
  }

  await logSync(entity, totalSaved);
  progressEnd(totalFetched, reportName);

  recordEntityStats(entity, {
    fetched: totalFetched,
    saved: totalSaved,
    failed: totalFailed,
    skipped,
    newCount: totalNew
  });

  structuredLog("info", `[${entity}] VALIDATE: ${totalFetched} fetched, ${skipped} skipped, ${processed} windows processed, ${resumed} windows resumed from checkpoint`);

  structuredLog("info", `[${entity}] COMPLETE: fetched=${totalFetched} saved=${totalSaved} new=${totalNew} failed=${totalFailed} skipped=${skipped} remaining=0`, {
    duration: formatDuration(Date.now() - t0)
  });
  const billDbCount = await getDbCount("bills", ACTIVE_COMPANY_GUID, "bill_type = $2", [billType]);
  devLog(`✅ ${reportName}: ${totalSaved} new (${billDbCount} in DB)`);

  try {
    await upsertSyncState(ACTIVE_COMPANY_GUID, entity, {
      lastSyncAt: new Date().toISOString(),
      recordCount: billDbCount,
      meta: { windowsProcessed: processed, windowsResumed: resumed }
    });
  } catch {}
}

// ============================================================
//  6. SYNC ORDERS
// ============================================================
async function syncOrders() {
  if (!ACTIVE_COMPANY_GUID) return;
  const t0 = Date.now();
  progressStart("Fetching Orders");

  const xml = `
<ENVELOPE>
 <HEADER>
  <VERSION>1</VERSION>
  <TALLYREQUEST>Export Data</TALLYREQUEST>
  <TYPE>Collection</TYPE>
  <ID>OrderCollection</ID>
 </HEADER>
 <BODY>
  <DESC>
   <STATICVARIABLES>
    <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
    <SVCURRENTCOMPANY>${ACTIVE_COMPANY_NAME}</SVCURRENTCOMPANY>
   </STATICVARIABLES>

   <TDL>
    <TDLMESSAGE>

     <COLLECTION NAME="OrderCollection">
      <TYPE>Voucher</TYPE>
      <FILTER>SalesPurchaseOrderFilter</FILTER>

      <FETCH>
        GUID,
        DATE,
        VOUCHERNUMBER,
        VOUCHERTYPENAME,
        PARTYLEDGERNAME,
        AMOUNT
      </FETCH>
     </COLLECTION>

     <SYSTEM TYPE="Formulae" NAME="SalesPurchaseOrderFilter">
       $VoucherTypeName = "Sales Order" OR
       $VoucherTypeName = "Purchase Order"
     </SYSTEM>

    </TDLMESSAGE>
   </TDL>
  </DESC>
 </BODY>
</ENVELOPE>
`;

  const res = await fetchTallyXml(xml, { entity: "orders" });

  if (DEBUG) console.log("📥 RAW ORDER XML:\n", res.data);

  const parsed = await parseStringPromise(res.data);

  const vouchers =
    parsed?.ENVELOPE?.BODY?.[0]?.DATA?.[0]?.COLLECTION?.[0]?.VOUCHER || [];

  const voucherArray = Array.isArray(vouchers) ? vouchers : [vouchers];

  devLog("📦 Orders Found:", voucherArray.length);
  progressUpdate(voucherArray.length);

  const rows = [];
  const orderGuids = [];
  let skipped = 0;

  for (const v of voucherArray) {
    const voucherType = v.VOUCHERTYPENAME?.[0];
    const orderGuid = v.GUID?.[0];
    if (!orderGuid) {
      skipped++;
      continue;
    }

    orderGuids.push(orderGuid);

    rows.push({
      company_guid: ACTIVE_COMPANY_GUID,
      order_guid: orderGuid,
      order_no: v.VOUCHERNUMBER?.[0],
      order_date: toISODate(normalizeText(v.DATE?.[0])),
      party_name: normalizeText(v.PARTYLEDGERNAME?.[0]),
      total_amount: parseTallyNumber(v.AMOUNT?.[0] || "0"),
      type: voucherType === "Sales Order" ? "Sales" : "Purchase",
      payload: v
    });
  }

  structuredLog("info", `[orders] VALIDATE: ${voucherArray.length} fetched, ${skipped} skipped, ${rows.length} valid`);

  const result = await writeRowsSafe({
    table: "orders",
    columns: [
      "company_guid",
      "order_guid",
      "order_no",
      "order_date",
      "party_name",
      "total_amount",
      "type",
      "payload"
    ],
    rows,
    conflict: ["company_guid", "order_guid"],
    updateColumns: [
      "order_no",
      "order_date",
      "party_name",
      "total_amount",
      "type",
      "payload"
    ],
    entity: "orders",
    idFn: (r) => r.order_guid
  });

  await cleanupByGuids("orders", ACTIVE_COMPANY_GUID, "order_guid", orderGuids);
  await logSync("orders", result.synced);
  progressEnd(rows.length, "Orders");

  const orderChecksum = computeGuidChecksum(orderGuids);
  const prevOrderState = await getSyncState(ACTIVE_COMPANY_GUID, "orders");
  const orderChanged = prevOrderState ? prevOrderState.checksum !== orderChecksum : true;

  recordEntityStats("orders", {
    fetched: voucherArray.length,
    saved: result.synced,
    failed: result.failed.length,
    skipped,
    newCount: result.inserted
  });

  structuredLog("info", `[orders] COMPLETE: fetched=${voucherArray.length} saved=${result.synced} new=${result.inserted} failed=${result.failed.length} skipped=${skipped} remaining=0 changed=${orderChanged ? "yes" : "no"}`, {
    duration: formatDuration(Date.now() - t0)
  });
  const orderDbCount = await getDbCount("orders", ACTIVE_COMPANY_GUID);
  devLog(`✅ Orders: ${result.synced} new (${orderDbCount} in DB)${orderChanged ? " ⚡ changes detected" : " (unchanged)"}`);

  try {
    await upsertSyncState(ACTIVE_COMPANY_GUID, "orders", {
      lastSyncAt: new Date().toISOString(),
      recordCount: voucherArray.length,
      checksum: orderChecksum,
      meta: { changed: orderChanged }
    });
  } catch {}
}

// ============================================================
//  7. INVENTORY — FULL STOCK
// ============================================================
async function fetchFullStockData() {
  if (!ACTIVE_COMPANY_GUID) return;
  const t0 = Date.now();
  progressStart("Fetching Inventory Items");

  const xml = `
<ENVELOPE>
 <HEADER>
  <VERSION>1</VERSION>
  <TALLYREQUEST>Export</TALLYREQUEST>
  <TYPE>Collection</TYPE>
  <ID>StockItemCollection</ID>
 </HEADER>
 <BODY>
  <DESC>
   <STATICVARIABLES>
    <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
    <SVCURRENTCOMPANY>${ACTIVE_COMPANY_NAME}</SVCURRENTCOMPANY>
   </STATICVARIABLES>
   <TDL>
    <TDLMESSAGE>
     <COLLECTION NAME="StockItemCollection">
      <TYPE>StockItem</TYPE>
      <FETCH>
        NAME,
        GUID,
        PARENT,
        BASEUNITS,
        OPENINGBALANCE,
        OPENINGVALUE,
        CLOSINGBALANCE,
        CLOSINGVALUE
      </FETCH>
     </COLLECTION>
    </TDLMESSAGE>
   </TDL>
  </DESC>
 </BODY>
</ENVELOPE>
`;

  const res = await fetchTallyXml(xml, { entity: "inventory_items" });

  const parsed = await parseStringPromise(res.data);

  const items =
    parsed?.ENVELOPE?.BODY?.[0]?.DATA?.[0]?.COLLECTION?.[0]?.STOCKITEM || [];

  const itemArray = Array.isArray(items) ? items : [items];

  devLog("📦 Stock items from Tally:", itemArray.length);
  progressUpdate(itemArray.length);

  const rows = [];
  const itemGuids = [];
  let skipped = 0;

  for (const i of itemArray) {
    const itemName = normalizeText(i.NAME?.[0]) || i.$?.NAME || null;
    if (!itemName) {
      skipped++;
      continue;
    }

    const itemGuid = normalizeText(i.GUID?.[0]) || i.$?.GUID || null;
    if (!itemGuid) {
      skipped++;
      continue;
    }

    itemGuids.push(itemGuid);

    rows.push({
      company_guid: ACTIVE_COMPANY_GUID,
      item_guid: itemGuid,
      name: itemName,
      item_group: normalizeText(i.PARENT?.[0]),
      unit: normalizeText(i.BASEUNITS?.[0]),
      opening_qty: parseTallyNumber(i.OPENINGBALANCE?.[0]),
      opening_value: parseTallyNumber(i.OPENINGVALUE?.[0]),
      closing_qty: parseTallyNumber(i.CLOSINGBALANCE?.[0]),
      closing_value: parseTallyNumber(i.CLOSINGVALUE?.[0]),
      payload: i
    });
  }

  structuredLog("info", `[inventory_items] VALIDATE: ${itemArray.length} fetched, ${skipped} skipped, ${rows.length} valid`);

  const result = await writeRowsSafe({
    table: "inventory_items",
    columns: [
      "company_guid",
      "item_guid",
      "name",
      "item_group",
      "unit",
      "opening_qty",
      "opening_value",
      "closing_qty",
      "closing_value",
      "payload"
    ],
    rows,
    conflict: ["company_guid", "item_guid"],
    updateColumns: [
      "name",
      "item_group",
      "unit",
      "opening_qty",
      "opening_value",
      "closing_qty",
      "closing_value",
      "payload"
    ],
    entity: "inventory_items",
    idFn: (r) => r.item_guid
  });

  await cleanupByGuids("inventory_items", ACTIVE_COMPANY_GUID, "item_guid", itemGuids);
  await logSync("inventory_items", result.synced);
  progressEnd(rows.length, "Inventory Items");

  const invChecksum = computeGuidChecksum(itemGuids);
  const prevInvState = await getSyncState(ACTIVE_COMPANY_GUID, "inventory_items");
  const invChanged = prevInvState ? prevInvState.checksum !== invChecksum : true;

  recordEntityStats("inventory_items", {
    fetched: itemArray.length,
    saved: result.synced,
    failed: result.failed.length,
    skipped,
    newCount: result.inserted
  });

  structuredLog("info", `[inventory_items] COMPLETE: fetched=${itemArray.length} saved=${result.synced} new=${result.inserted} failed=${result.failed.length} skipped=${skipped} remaining=0 changed=${invChanged ? "yes" : "no"}`, {
    duration: formatDuration(Date.now() - t0)
  });
  const invDbCount = await getDbCount("inventory_items", ACTIVE_COMPANY_GUID);
  devLog(`✅ Full inventory: ${result.synced} new (${invDbCount} in DB)${invChanged ? " ⚡ changes detected" : " (unchanged)"}`);

  try {
    await upsertSyncState(ACTIVE_COMPANY_GUID, "inventory_items", {
      lastSyncAt: new Date().toISOString(),
      recordCount: itemArray.length,
      checksum: invChecksum,
      meta: { changed: invChanged }
    });
  } catch {}
}

// ============================================================
//  MAIN SYNC ORCHESTRATOR
// ============================================================
async function syncAllData(companies, dataTypes = "all") {
  if (isSyncRunning) {
    console.log("⏳ Previous sync still running. Skipping...");
    return;
  }

  isSyncRunning = true;
  const syncStart = Date.now();
  SYNC_RUN_ID = new Date().toISOString().replace(/[:.]/g, "-");
  SYNC_SUMMARY.startedAt = new Date();
  SYNC_SUMMARY.endedAt = null;
  SYNC_SUMMARY.companiesSynced = 0;
  SYNC_SUMMARY.companyGuids = [];
  SYNC_SUMMARY.entities = {};
  SYNC_SUMMARY.voucherTypeBreakdown = null;
  SYNC_SUMMARY.companies = [];
  CURRENT_COMPANY_STATS = null;

  const wants = (key) => dataTypes === "all" || dataTypes.includes(key);
  const selectedLabel = dataTypes === "all"
    ? "ALL"
    : dataTypes.join(", ");

  structuredLog("info", "Sync started", { companies: companies.length, dataTypes: selectedLabel });
  console.log("🔄 Sync started...");

  try {
    recordEntityStats("companies", {
      fetched: companies.length,
      saved: companies.length,
      failed: 0,
      skipped: 0
    });

    for (const company of companies) {
      const companyStart = Date.now();
      console.log("\n🔐 Syncing company:", company.name);
      structuredLog("info", `[company] Sync started: ${company.name}`, {
        companyGuid: company.company_guid
      });

      ACTIVE_COMPANY_GUID = company.company_guid;
      ACTIVE_COMPANY_NAME = company.name;
      SYNC_SUMMARY.companyGuids.push(company.company_guid);

      // Per-company summary bucket — each company gets its own completely
      // separate Fetched/New/Failed/Skipped/In DB counts and voucher breakdown.
      CURRENT_COMPANY_STATS = {
        guid: company.company_guid,
        name: company.name,
        entities: {},
        voucherTypeBreakdown: null
      };
      SYNC_SUMMARY.companies.push(CURRENT_COMPANY_STATS);

      if (wants("ledgers")) {
        devLog("➡️ syncLedgers");
        await syncLedgers(company);
      }

      if (wants("bills_receivable")) {
        devLog("➡️ syncBills (Receivable)");
        await syncBills(company);
      }

      if (wants("bills_payable")) {
        devLog("➡️ syncBills (Payable)");
        await syncBillsPayable(company);
      }

      // Vouchers and invoices share a single "Voucher Register" Tally request;
      // run it once whenever either is selected and save only what was chosen.
      if (wants("vouchers") || wants("invoices")) {
        devLog("➡️ syncVouchers");
        await syncVouchers(company, {
          saveVouchers: wants("vouchers"),
          saveInvoices: wants("invoices")
        });
      }

      if (wants("orders")) {
        devLog("➡️ syncOrders");
        await syncOrders();
      }

      if (wants("inventory_items")) {
        devLog("➡️ fetchFullStockData");
        await fetchFullStockData();
      }

      structuredLog("info", `[company] Sync completed: ${company.name}`, {
        duration: formatDuration(Date.now() - companyStart)
      });
      console.log(`✅ Sync completed for ${company.name}`);
      SYNC_SUMMARY.companiesSynced++;
    }

    CURRENT_COMPANY_STATS = null;

    SYNC_SUMMARY.endedAt = new Date();
    await printSyncSummary(syncStart);

    console.log("\n✅ Sync complete!");
  } catch (err) {
    SYNC_SUMMARY.endedAt = new Date();
    await printSyncSummary(syncStart);
    structuredLog("error", "Sync FAILED", { error: err.message, stack: err.stack });
    console.error("⛔ SYNC FAILED");
    console.error("MESSAGE:", err.message);
  } finally {
    isSyncRunning = false;
  }
}

// ============================================================
//  INTERACTIVE COMPANY SELECTION
// ============================================================
async function selectCompanies(allCompanies) {
  console.log("\n🏢 Companies available for sync:");
  allCompanies.forEach((c, i) => {
    console.log(`   [${i + 1}] ${c.name}`);
  });
  console.log("   [0] ALL companies");

  const answer = await askQuestion(
    "\nSelect company number(s) (comma separated, e.g. 1,3), '0' for all, or B to go back: "
  );

  const trimmed = answer.trim().toLowerCase();

  if (trimmed === "b") {
    return null;
  }

  if (trimmed === "0" || trimmed === "all") {
    console.log(`✅ Selected ALL companies (${allCompanies.length})`);
    return allCompanies;
  }

  const selected = [];
  for (const part of trimmed.split(",")) {
    const idx = parseInt(part.trim(), 10);
    if (!isNaN(idx) && idx >= 1 && idx <= allCompanies.length) {
      selected.push(allCompanies[idx - 1]);
    }
  }

  if (!selected.length) {
    console.log("⚠️ No valid selection. Defaulting to ALL companies.");
    return allCompanies;
  }

  console.log(
    "✅ Selected:",
    selected.map((c) => c.name).join(", ")
  );
  return selected;
}

// ============================================================
//  INTERACTIVE DATA-TYPE SELECTION
// ============================================================
const DATA_TYPES = [
  { key: "ledgers", label: "Ledgers" },
  { key: "bills_receivable", label: "Bills Receivable" },
  { key: "bills_payable", label: "Bills Payable" },
  { key: "vouchers", label: "Vouchers" },
  { key: "invoices", label: "Invoices" },
  { key: "orders", label: "Orders" },
  { key: "inventory_items", label: "Inventory Items" }
];

async function selectDataTypes() {
  console.log("\n📊 Data to sync:");
  DATA_TYPES.forEach((d, i) => {
    console.log(`   [${i + 1}] ${d.label}`);
  });
  console.log("   [0] ALL data");

  const answer = await askQuestion(
    "\nSelect data type number(s) (comma separated, e.g. 2,4), '0' for ALL data, or B to go back: "
  );

  const trimmed = answer.trim().toLowerCase();

  if (trimmed === "b") {
    return null;
  }

  if (trimmed === "0" || trimmed === "all") {
    console.log("✅ Selected ALL data types");
    return "all";
  }

  const selected = [];
  for (const part of trimmed.split(",")) {
    const idx = parseInt(part.trim(), 10);
    if (!isNaN(idx) && idx >= 1 && idx <= DATA_TYPES.length) {
      selected.push(DATA_TYPES[idx - 1].key);
    }
  }

  if (!selected.length) {
    console.log("⚠️ No valid selection. Defaulting to ALL data types.");
    return "all";
  }

  console.log(
    "✅ Selected:",
    selected.map((k) => DATA_TYPES.find((d) => d.key === k).label).join(", ")
  );
  return selected;
}

// ============================================================
//  SYNC FLOW (company selection + data type selection + timer + sync loop)
// ============================================================
async function runSyncFlow() {
  try {
    SYNC_RUN_ID = new Date().toISOString().replace(/[:.]/g, "-");

    // Step 1: Detect companies + auto-create in DB
    const companies = await detectCompanies();

    if (!companies.length) {
      console.error("❌ No companies found in Tally.");
      return;
    }

    // Steps 2-4: pre-sync configuration screens. Typing B steps back one
    // screen; backing out of company selection returns to the main menu.
    let selectedCompanies = null;
    let selectedDataTypes = null;
    let minutesAnswer = "";

    while (true) {
      // Step 2: Let the user pick one company or ALL
      selectedCompanies = await selectCompanies(companies);
      if (!selectedCompanies) {
        console.log("↩️ Back to main menu.\n");
        return;
      }

      // Step 3: Let the user pick which data types to sync (one or many)
      selectedDataTypes = await selectDataTypes();
      if (!selectedDataTypes) {
        console.log("\n↩️ Back to company selection.\n");
        continue;
      }

      // Step 4: Auto-sync interval (minutes). 0 = keep syncing continuously
      // (defaults to a 5 minute gap between runs).
      minutesAnswer = await askQuestion(
        "\nEnter sync interval in minutes (0 = continuous, every 5 min, B = back): "
      );
      if (minutesAnswer.trim().toLowerCase() === "b") {
        console.log("\n↩️ Back to data selection.\n");
        continue;
      }
      break;
    }

    const minutes = parseFloat(minutesAnswer) || 0;
    const loopWaitMs = minutes > 0 ? minutes * 60 * 1000 : 5 * 60 * 1000;
    const loopMinutes = Math.round(loopWaitMs / 60000);
    SYNC_INTERVAL = loopWaitMs;

    if (minutes > 0) {
      console.log(`⏱ Sync will repeat every ${minutes} minute(s)\n`);
    } else {
      console.log("⏱ Running continuous sync (every 5 min). Press Ctrl+C to stop.\n");
    }

    await syncAllData(selectedCompanies, selectedDataTypes);

    // Step 5: Endless auto-sync loop. It NEVER stops on its own — if a sync run
    // fails, we log it and keep going until the terminal is closed (Ctrl+C).
    console.log(`\n🔁 Auto-sync loop started (every ${loopMinutes} min). It will keep syncing until you stop the terminal. Press Ctrl+C to stop.`);
    while (true) {
      try {
        await new Promise((r) => setTimeout(r, loopWaitMs));
        const heapMb = Math.round((process.memoryUsage().heapUsed / 1024 / 1024) * 10) / 10;
        structuredLog("info", `[auto-sync] waiting ${loopMinutes} min before next sync (heap ${heapMb} MB)`);
        await syncAllData(selectedCompanies, selectedDataTypes);
      } catch (err) {
        structuredLog("error", "[auto-sync] Loop iteration failed; continuing", {
          error: err.message,
          stack: err.stack
        });
      }
    }
  } catch (err) {
    console.error("⛔ FATAL ERROR:", err.message);
  }
}

// ============================================================
//  RESET & FULL RE-SYNC
//  Clears all checkpoints and sync_state so the next sync
//  fetches EVERYTHING from Tally fresh with the latest filters
//  (cancelled voucher exclusion, etc.).
// ============================================================
async function resetAndFullSync() {
  console.log("\n⚠️  RESET & FULL RE-SYNC");
  console.log("This will:");
  console.log("  • Delete all checkpoint/resume data");
  console.log("  • Clear sync_state (incremental metadata)");
  console.log("  • Re-fetch ALL data from Tally on next sync");
  console.log("  • Apply cancelled voucher filter to ALL vouchers\n");

  const confirm = await askQuestion("Type 'YES' to confirm reset (B to cancel): ");
  if (confirm.trim().toUpperCase() !== "YES") {
    console.log("❌ Reset cancelled. Nothing was cleared.\n");
    return;
  }

  // 1. Delete checkpoint file
  try {
    if (fs.existsSync(SYNC_STATE_FILE)) {
      fs.unlinkSync(SYNC_STATE_FILE);
      console.log("  ✅ Deleted checkpoint file:", SYNC_STATE_FILE);
    } else {
      console.log("  ℹ️  No checkpoint file found (already clean)");
    }
  } catch (err) {
    console.log("  ⚠️ Could not delete checkpoint file:", err.message);
  }

  // 2. Clear sync_state table
  try {
    await pool.query("DELETE FROM sync_state");
    console.log("  ✅ Cleared sync_state table");
  } catch (err) {
    console.log("  ⚠️ Could not clear sync_state:", err.message);
  }

  // 3. Optionally clear old vouchers so re-sync is clean
  const clearDb = await askQuestion("\nAlso DELETE all existing vouchers/invoices from DB? (recommended for accurate counts) [y/N]: ");
  if (clearDb.trim().toLowerCase() === "y" || clearDb.trim().toLowerCase() === "yes") {
    try {
      console.log("  🗑️  Deleting invoices...");
      await pool.query("DELETE FROM invoices");
      console.log("  🗑️  Deleting vouchers...");
      await pool.query("DELETE FROM vouchers");
      console.log("  🗑️  Deleting sync_log...");
      await pool.query("DELETE FROM sync_log");
      console.log("  ✅ All voucher/invoice data cleared");
    } catch (err) {
      console.log("  ⚠️ Could not clear tables:", err.message);
    }
  }

  console.log("\n✅ Reset complete! Starting full re-sync...\n");

  // 4. Trigger a full sync
  await runSyncFlow();
}

// ============================================================
//  BOOTSTRAP
// ============================================================
async function bootstrap() {
  console.log("\n🟢 TallyScrapper Middleware starting...\n");

  // ── Step 1: Authentication Menu ──────────────────────────
  await startAuthenticationFlow();

  // ── Step 2: Connect to database ──────────────────────────
  let cfg = loadDbConfig();

  if (cfg) {
    console.log("📂 Saved database configuration found. Connecting...");
    try {
      await connectWithConfig(cfg);
      console.log("✅ Connected automatically to the saved database.\n");
    } catch (err) {
      console.log(`⚠️ Could not connect using the saved configuration: ${err.message}`);
      console.log("Please update your database settings.\n");
      cfg = await dbWizard();
      while (!cfg) {
        console.log("⚠️ A working database connection is required to continue.");
        cfg = await dbWizard();
      }
      await connectWithConfig(cfg);
      console.log("✅ Connected with new settings.\n");
    }
  } else {
    console.log("🛠️ First launch detected — let's set up your database.\n");
    cfg = await dbWizard();
    while (!cfg) {
      console.log("⚠️ A working database connection is required to continue.");
      cfg = await dbWizard();
    }
    await connectWithConfig(cfg);
  }

  // ── Step 3: Main menu loop ───────────────────────────────
  let running = true;

  while (running) {
    const choice = await mainMenu();

    if (choice === "1") {
      await runSyncFlow();
    } else if (choice === "2") {
      console.log("\n🛠️ Change Database Configuration\n");
      const newCfg = await dbWizard();
      if (!newCfg) {
        console.log("↩️ Returned to main menu without changing the database.\n");
      } else {
        try {
          await connectWithConfig(newCfg);
          console.log("✅ Database switched successfully.\n");
        } catch (err) {
          console.error(`❌ Could not connect with the new settings: ${err.message}`);
        }
      }
    } else if (choice === "3") {
      await tallyPortWizard();
    } else if (choice === "4") {
      await resetAndFullSync();
    } else if (choice === "5") {
      running = false;
    } else {
      console.log("⚠️ Invalid option. Please choose 1-5.");
    }
  }

  await pool.end();
  console.log("👋 Goodbye!");
  process.exit(0);
}

// When required as a module (tests / automation), do NOT boot the interactive CLI.
if (require.main === module) {
  bootstrap().catch((err) => {
    structuredLog("error", "Bootstrap FAILED", { error: err.message });
    console.error("⛔ BOOTSTRAP FAILED:", err.message);
    process.exit(1);
  });
}

module.exports = {
  bootstrap,
  isDeveloperModeEnabled: () => isDeveloperMode,
  developerLogin,
  setActiveCompany: (guid, name) => {
    ACTIVE_COMPANY_GUID = guid;
    ACTIVE_COMPANY_NAME = name;
  },
  getActiveCompany: () => ({ guid: ACTIVE_COMPANY_GUID, name: ACTIVE_COMPANY_NAME }),
  loadDbConfig,
  connectWithConfig,
  getPool: () => pool,
  detectCompanies,
  saxVoucherStream,
  fetchVouchersStream,
  saxBillsStream,
  fetchBillsStream,
  syncLedgers,
  syncVouchers,
  syncOrders,
  fetchFullStockData,
  syncBills,
  syncBillsPayable,
  syncBillsForType,
  resolveSyncStartYear,
  getVoucherWindows,
  getBillsWindows,
  syncAllData,
  selectDataTypes,
  selectCompanies,
  DATA_TYPES,
  getCompanyCheckpoint,
  setCompanyCheckpoint,
  getBillSyncState,
  saveBillSyncState,
  loadSyncState,
  saveSyncState,
  normalizeTallyDate,
  parseStartingFromName,
  fmtYMD
};
