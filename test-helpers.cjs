const fs = require("fs");

const src = fs.readFileSync("agent.cjs", "utf8");

function extractFunction(name) {
  const re = new RegExp(`(?:^|\\n)(?:async )?function ${name}\\(`);
  const match = re.exec(src);
  if (!match) throw new Error(`function ${name} not found`);
  const start = match.index + match[0].replace(/^\n/, "").length - match[0].length + match[0].indexOf("(");
  let braceCount = 0;
  let i = start;
  let started = false;
  while (i < src.length) {
    const ch = src[i];
    if (ch === "{") {
      started = true;
      braceCount++;
    } else if (ch === "}") {
      braceCount--;
    }
    if (started && braceCount === 0) break;
    i++;
  }
  if (!started || braceCount !== 0) throw new Error(`cannot extract ${name}`);
  return src.slice(match.index, i + 1);
}

const names = [
  "fmtYMD",
  "getVoucherWindows",
  "getBillsWindows",
  "resolveSyncStartYear",
  "parseStartingFromName",
  "normalizeTallyDate"
];

const block = `(() => {\nconst SHORT_MONTHS = {\n  jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6,\n  jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12\n};\n${names
  .map((n) => extractFunction(n))
  .join("\n")}\nreturn { ${names.join(", ")} };\n})()`;
const funcs = eval(block);

const assert = (cond, msg) => {
  if (!cond) {
    console.error("FAIL:", msg);
    process.exitCode = 1;
  } else {
    console.log("ok:", msg);
  }
};

// --- fmtYMD ---
assert(funcs.fmtYMD(new Date(2016, 3, 1)) === "20160401", "fmtYMD April 2016");

// --- getVoucherWindows ---
const windows = funcs.getVoucherWindows(new Date(2016, 3, 1), 5);
assert(windows.length > 0, "windows non-empty");
assert(windows[0].fromDate === "20160401", "first window fromDate");
assert(windows[0].toDate === "20160405", "first window toDate");
assert(windows[0].label === "20160401", "first window label");
const last = windows[windows.length - 1];
assert(last.toDate <= new Date().getFullYear() * 10000 + 1231, "last window ends by now");

// --- getBillsWindows ---
const billWindows = funcs.getBillsWindows(2016);
assert(billWindows.length >= 10, `bills windows cover 10+ years (${billWindows.length})`);
assert(billWindows[0].fromDate === "20160401", "bills first window fromDate April");
assert(billWindows[0].label === "20160401", "bills first window label");
assert(billWindows[0].toDate >= "20170301", "bills first window ends in March of next FY");
assert(billWindows[billWindows.length - 1].toDate >= funcs.fmtYMD(new Date()).slice(0, 4) + "0101", "bills last window reaches current year");

// --- resolveSyncStartYear ---
const origEnv = process.env.SYNC_START_YEAR;
process.env.SYNC_START_YEAR = "2005";
assert(funcs.resolveSyncStartYear({}) === 2005, "env SYNC_START_YEAR wins");
delete process.env.SYNC_START_YEAR;
assert(funcs.resolveSyncStartYear({ startingFrom: "2016-04-01" }) === 2016, "startingFrom used");
assert(funcs.resolveSyncStartYear({}) === 1900, "fallback to full history (1900)");
if (origEnv !== undefined) process.env.SYNC_START_YEAR = origEnv;

// --- parseStartingFromName ---
assert(funcs.parseStartingFromName("Rajlaxmi Solutions Private Limited - (From 1-Apr-2016)") === "2016-04-01", "parse name From");
assert(funcs.parseStartingFromName("Tally") === null, "no From suffix -> null");

// --- normalizeTallyDate ---
assert(funcs.normalizeTallyDate("01-04-2016") === "2016-04-01", "DD-MM-YYYY");
assert(funcs.normalizeTallyDate("1-Apr-2016") === "2016-04-01", "DD-Mon-YYYY");
assert(funcs.normalizeTallyDate("20160401") === "2016-04-01", "YYYYMMDD");
assert(funcs.normalizeTallyDate("2016-04-01") === "2016-04-01", "already ISO");
assert(funcs.normalizeTallyDate(null) === null, "null safe");
assert(funcs.normalizeTallyDate("garbage") === null, "garbage safe");

// --- checkpoint startYear invalidation logic (mirror of sync code) ---
function skipWindow(checkpointStartYear, syncStartYear, lastWindow, label) {
  const lw = checkpointStartYear === syncStartYear ? lastWindow || null : null;
  return !!(lw && label <= lw);
}
assert(skipWindow(2024, 2024, "20260115", "20250110") === true, "same startYear -> resume skips");
assert(skipWindow(2024, 2016, "20260115", "20250110") === false, "changed startYear -> resync old windows");
assert(skipWindow(null, 2026, "20260115", "20250110") === false, "no stored startYear -> no skip");

console.log("\nDONE");
