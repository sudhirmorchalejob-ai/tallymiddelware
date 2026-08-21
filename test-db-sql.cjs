const fs = require("fs");
const path = require("path");
const os = require("os");
const crypto = require("crypto");
const { Pool } = require("pg");

function getEncryptionKey() { const seed = `${os.hostname()}|${os.platform()}|${os.arch()}|${os.userInfo().username}`; return crypto.scryptSync(seed, "tally-agent-config-v1", 32); }
function decryptConfig(raw) {
  const key = getEncryptionKey();
  const parsed = JSON.parse(raw);
  const decipher = crypto.createDecipheriv("aes-256-gcm", key, Buffer.from(parsed.iv, "base64"));
  decipher.setAuthTag(Buffer.from(parsed.tag, "base64"));
  return Buffer.concat([decipher.update(Buffer.from(parsed.data, "base64")), decipher.final()]).toString("utf8");
}

(async () => {
  const cfg = JSON.parse(decryptConfig(fs.readFileSync(path.join(os.homedir(), "tally-agent-db-config.enc"), "utf8")));
  const pool = new Pool({ host: cfg.host, port: cfg.port, user: cfg.user, password: cfg.password, database: cfg.database, max: 1 });

  const bigCompany = "44acb1ff-18e7-40de-822e-c7f32ad504e4";
  const smallCompany = "e479dada-7f3e-4719-8970-d92863523876";

  // Validate the exact cleanupWindow SQL (in a transaction that we ROLL BACK,
  // so no data is actually changed).
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const delRes = await client.query(
      `DELETE FROM vouchers
        WHERE company_guid = $1
          AND voucher_date >= $2::date
          AND voucher_date <= $3::date
          AND NOT (voucher_guid = ANY($4::text[]))
        RETURNING voucher_guid`,
      [bigCompany, "20250101", "20250131", ["abc-def"]]
    );
    console.log("cleanupWindow SQL valid; would-remove rows (rolled back):", delRes.rowCount);
    await client.query("ROLLBACK");
  } catch (e) {
    console.error("FAIL:", e.message);
  } finally {
    client.release();
  }

  // Validate the per-batch invoices DELETE SQL shape.
  try {
    await client.query("BEGIN");
    const r2 = await client.query(
      "DELETE FROM invoices WHERE company_guid = $1 AND invoice_guid = ANY($2::text[]) RETURNING invoice_guid",
      [smallCompany, ["does-not-exist"]]
    );
    console.log("invoices DELETE valid:", r2.rowCount);
    await client.query("ROLLBACK");
  } catch (e) {
    console.error("FAIL:", e.message);
  }

  await pool.end();
})();
