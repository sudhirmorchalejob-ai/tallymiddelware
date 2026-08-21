const fs = require("fs");

const src = fs.readFileSync("agent.cjs", "utf8");

function extractFunction(name) {
  const re = new RegExp(`(?:^|\\n)(?:async )?function ${name}\\(`);
  const match = re.exec(src);
  if (!match) throw new Error(`function ${name} not found`);
  const start = match.index + match[0].indexOf("(");
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

const sax = require("sax");
const block = `(() => {\nconst sax = global.__sax;\n${extractFunction("saxBillsStream")}\nreturn { saxBillsStream };\n})()`;
global.__sax = sax;
const { saxBillsStream } = eval(block);

const assert = (cond, msg) => {
  if (!cond) {
    console.error("FAIL:", msg);
    process.exitCode = 1;
  } else {
    console.log("ok:", msg);
  }
};

function feed(parser, xml) {
  parser.write(xml);
  parser.close();
}

function scalar(val) {
  return Array.isArray(val) ? val[0] : val;
}

// --- 1. Interleaved bills (BILLFIXED/BILLCL/BILLDUE/BILLOVERDUE per bill) ---
{
  const { parser, result } = saxBillsStream();
  feed(
    parser,
    `<?xml version="1.0"?>
<ENVELOPE>
 <BODY>
  <DATA>
   <BILLFIXED><BILLPARTY>Acme Traders</BILLPARTY><BILLREF>INV-001</BILLREF><BILLDATE>2016-04-15</BILLDATE></BILLFIXED>
   <BILLCL>12500.00</BILLCL>
   <BILLDUE>2016-05-15</BILLDUE>
   <BILLOVERDUE>0</BILLOVERDUE>
   <BILLFIXED><BILLPARTY>Beta Corp</BILLPARTY><BILLREF>INV-002</BILLREF><BILLDATE>2016-04-20</BILLDATE></BILLFIXED>
   <BILLCL>800.50</BILLCL>
   <BILLDUE>2016-05-20</BILLDUE>
   <BILLOVERDUE>12</BILLOVERDUE>
  </DATA>
 </BODY>
</ENVELOPE>`
  );
  assert(result.fixed.length === 2, "interleaved: 2 BILLFIXED");
  assert(result.cl.length === 2, "interleaved: 2 BILLCL");
  assert(scalar(result.fixed[0].BILLPARTY) === "Acme Traders", "interleaved: bill[0] party");
  assert(scalar(result.fixed[1].BILLREF) === "INV-002", "interleaved: bill[1] ref");
  assert(scalar(result.cl[1]) === "800.50", "interleaved: cl[1] amount");
  assert(scalar(result.over[1]) === "12", "interleaved: over[1] days");
}

// --- 2. Grouped layout (all BILLFIXED first, then all amounts) ---
{
  const { parser, result } = saxBillsStream();
  feed(
    parser,
    `<ENVELOPE>
 <BODY>
  <DATA>
   <BILLFIXED><BILLPARTY>Party A</BILLPARTY><BILLREF>B1</BILLREF></BILLFIXED>
   <BILLFIXED><BILLPARTY>Party B</BILLPARTY><BILLREF>B2</BILLREF></BILLFIXED>
   <BILLCL>100</BILLCL>
   <BILLCL>200</BILLCL>
   <BILLDUE>2020-01-01</BILLDUE>
   <BILLDUE>2020-02-01</BILLDUE>
  </DATA>
 </BODY>
</ENVELOPE>`
  );
  assert(result.fixed.length === 2 && result.cl.length === 2, "grouped: arrays parallel");
  assert(scalar(result.cl[0]) === "100", "grouped: cl[0]");
  assert(scalar(result.cl[1]) === "200", "grouped: cl[1]");
  assert(scalar(result.due[1]) === "2020-02-01", "grouped: due[1]");
}

// --- 3. Top-level under ENVELOPE (no BODY/DATA) ---
{
  const { parser, result } = saxBillsStream();
  feed(
    parser,
    `<ENVELOPE>
   <BILLFIXED><BILLPARTY>Direct</BILLPARTY><BILLREF>X1</BILLREF></BILLFIXED>
   <BILLCL>42</BILLCL>
</ENVELOPE>`
  );
  assert(result.fixed.length === 1 && scalar(result.fixed[0].BILLPARTY) === "Direct", "top-level: detected at any depth");
  assert(scalar(result.cl[0]) === "42", "top-level: cl detected");
}

// --- 4. LINEERROR ---
{
  const { parser, result } = saxBillsStream();
  feed(parser, `<ENVELOPE><LINEERROR></LINEERROR></ENVELOPE>`);
  assert(parser.seenLineError === true, "LINEERROR flagged");
  assert(result.fixed.length === 0, "LINEERROR: zero bills");
}

// --- 5. Chunk-boundary splitting (XML element split across writes) ---
{
  const { parser, result } = saxBillsStream();
  const xml =
    `<ENVELOPE><BODY><DATA>` +
    `<BILLFIXED><BILLPARTY>Split Test</BILLPARTY><BILLREF>SPLIT-1</BILLREF><BILLDATE>2019-06-01</BILLDATE></BILLFIXED>` +
    `<BILLCL>999.99</BILLCL>` +
    `</DATA></BODY></ENVELOPE>`;
  for (let i = 0; i < xml.length; i += 7) {
    parser.write(xml.slice(i, i + 7));
  }
  parser.close();
  assert(result.fixed.length === 1, "split: one bill parsed");
  assert(scalar(result.fixed[0].BILLREF) === "SPLIT-1", "split: ref preserved across chunk boundaries");
  assert(scalar(result.cl[0]) === "999.99", "split: amount preserved");
}

// --- 6. Nested sub-elements collapse to object with array values (like VOUCHER parse) ---
{
  const { parser, result } = saxBillsStream();
  feed(
    parser,
    `<ENVELOPE>
  <BILLFIXED>
    <BILLPARTY>Nested</BILLPARTY>
    <BILLREF>N-1</BILLREF>
    <BILLDATE>2021-03-31</BILLDATE>
    <BILLAMOUNT>5000</BILLAMOUNT>
  </BILLFIXED>
  <BILLCL>5000</BILLCL>
</ENVELOPE>`
  );
  assert(scalar(result.fixed[0].BILLDATE) === "2021-03-31", "nested: BILLDATE read");
  assert(scalar(result.fixed[0].BILLAMOUNT) === "5000", "nested: extra BILLAMOUNT read");
}

console.log("DONE");
