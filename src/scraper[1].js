// src/scraper.js
// Multi-barn livestock scraper. Supports:
//   - SFRL (HTML tables)
//   - Pipestone (HTML inline text)
//   - Glacial Lakes (PDF linked from results page)
//   - Rock Creek (PDF linked from market reports page)

const fetch   = require("node-fetch");
const cheerio = require("cheerio");
const pdf     = require("pdf-parse");
const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

// ─── Barn configs ─────────────────────────────────────────────────────────
const BARNS = [
  {
    id:      "sfrl",
    name:    "Sioux Falls Regional Livestock",
    url:     "https://sfrlinc.com/web/market-reports/",
    parser:  "sfrl_html",
  },
  {
    id:      "pipestone",
    name:    "Pipestone Livestock Auction",
    urls: [
      "https://www.pipestonelivestock.com/feeder-cattle",
      "https://www.pipestonelivestock.com/slaughter-fat-bulls",
      "https://www.pipestonelivestock.com/sheep-goats",
    ],
    parser:  "pipestone_html",
  },
  {
    id:      "glacial",
    name:    "Glacial Lakes Livestock",
    resultsUrl: "https://glaciallakeslive.com/results",
    parser:  "pdf",
  },
  {
    id:      "rockcreek",
    name:    "Rock Creek Livestock Market",
    resultsUrl: "https://rockcreeklivestockmarket.com/?page_id=348",
    parser:  "pdf",
  },
];

// ─── Shared helpers ───────────────────────────────────────────────────────
async function fetchPage(url) {
  console.log(`[scraper] Fetching ${url}`);
  const res = await fetch(url, {
    headers: { "User-Agent": "Mozilla/5.0 (compatible; LivestockScraper/1.0)" },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} fetching ${url}`);
  return res.text();
}

async function fetchBinary(url) {
  console.log(`[scraper] Fetching PDF ${url}`);
  const res = await fetch(url, {
    headers: { "User-Agent": "Mozilla/5.0 (compatible; LivestockScraper/1.0)" },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} fetching PDF ${url}`);
  return res.buffer();
}

function classifySaleType(text) {
  const t = text.toUpperCase();
  if (t.includes("FEEDER"))                                               return "Feeder Cattle";
  if (t.includes("SLAUGHTER") || t.includes("SLTR"))                     return "Slaughter";
  if (t.includes("BRED") || t.includes("COW/CALF") || t.includes("C/C PAIR")) return "Bred Cows";
  if (t.includes("FAT") || t.includes("FED CATTLE") || t.includes("FINISHED")) return "Fed Cattle";
  if (t.includes("SHEEP") || t.includes("LAMB") || t.includes("GOAT") || t.includes("KID")) return "Sheep & Goats";
  return "Other";
}

const PER_HEAD_CLASSES = ["BRED COW", "BRED HEIFER", "C/C PAIR", "COW/CALF", "PAIR"];
function isPerHead(saleType, cls) {
  return saleType === "Bred Cows" ||
    PER_HEAD_CLASSES.some(c => cls.toUpperCase().includes(c));
}

function extractDate(str) {
  const m1 = str.match(/\((\d{1,2}\/\d{1,2}\/\d{2,4})\)/);
  if (m1) {
    const parts = m1[1].split("/");
    const yy = parts[2].length === 4 ? parts[2].slice(2) : parts[2];
    return `${parts[0].padStart(2,"0")}/${parts[1].padStart(2,"0")}/${yy}`;
  }
  const m2 = str.match(/(\d{1,2})\.(\d{1,2})\.(\d{2,4})/);
  if (m2) {
    const yy = m2[3].length === 4 ? m2[3].slice(2) : m2[3];
    return `${m2[1].padStart(2,"0")}/${m2[2].padStart(2,"0")}/${yy}`;
  }
  return null;
}

// ─── SFRL HTML parser ─────────────────────────────────────────────────────
function parseSFRL(html, barnId) {
  const $ = cheerio.load(html);
  const lots = [];
  let currentSaleType = "Feeder Cattle";
  let currentDate     = null;

  $("h3, h4, table").each((_, el) => {
    const tag  = el.tagName.toLowerCase();
    const text = $(el).text().trim();

    if (tag === "h3" || tag === "h4") {
      const upper = text.toUpperCase();
      if (upper.includes("FEEDER") || upper.includes("SLAUGHTER") ||
          upper.includes("BRED")   || upper.includes("FED CATTLE") ||
          upper.includes("FAT")    || upper.includes("SHEEP") ||
          upper.includes("LAMB")   || upper.includes("GOAT")) {
        currentSaleType = classifySaleType(text);
        const d = extractDate(text);
        if (d) currentDate = d;
      }
      return;
    }

    $(el).find("tr").each((_, row) => {
      const cells = $(row).find("td").map((_, td) => $(td).text().trim()).get();
      if (cells.length < 5) return;
      const head   = parseInt(cells[0], 10);
      const color  = cells[1].toUpperCase().trim();
      const cls    = cells[2].toUpperCase().trim();
      const weight = parseFloat(cells[3].replace(/,/g, ""));
      const price  = parseFloat(cells[4].replace(/[$,]/g, ""));
      if (!head || isNaN(weight) || isNaN(price) || !cls) return;
      lots.push({ head, color, class: cls, weight, price,
        sale_type: currentSaleType, sale_date: currentDate || "unknown",
        per_head: isPerHead(currentSaleType, cls), barn_id: barnId });
    });
  });

  console.log(`[scraper] SFRL: parsed ${lots.length} lots`);
  return lots;
}

// ─── Pipestone HTML parser ────────────────────────────────────────────────
function parsePipestone(html, saleType, barnId) {
  const $ = cheerio.load(html);
  const lots = [];

  let saleDate = null;
  $("h1,h2,h3,h4,p").each((_, el) => {
    if (!saleDate) {
      const d = extractDate($(el).text());
      if (d) saleDate = d;
    }
  });

  const text = $("body").text();

  // Pattern: "528#.... $427.50 Blk 4 head"
  const lotPattern = /(\d+)#[.\s]+\$?([\d,]+\.?\d*)\s+(\w+)\s+(\d+)\s+head/gi;
  let m;
  while ((m = lotPattern.exec(text)) !== null) {
    const weight = parseFloat(m[1]);
    const price  = parseFloat(m[2].replace(/,/g, ""));
    const color  = m[3].toUpperCase();
    const head   = parseInt(m[4], 10);
    if (!head || isNaN(weight) || isNaN(price)) continue;
    const cls = saleType === "Slaughter"   ? "COW" :
                saleType === "Fed Cattle"  ? "FAT STEER" :
                saleType === "Sheep & Goats" ? "LAMB" : "STEER";
    lots.push({ head, color, class: cls, weight, price,
      sale_type: saleType, sale_date: saleDate || "unknown",
      per_head: isPerHead(saleType, cls), barn_id: barnId });
  }

  console.log(`[scraper] Pipestone (${saleType}): parsed ${lots.length} lots`);
  return lots;
}

// ─── PDF parser (Glacial Lakes + Rock Creek) ──────────────────────────────
async function parsePDF(pdfBuffer, barnId, dateHint) {
  const data = await pdf(pdfBuffer);
  const text = data.text;
  const lots = [];

  let currentSaleType = "Feeder Cattle";
  let currentDate     = dateHint || null;
  let currentClass    = "STEER";

  const lines = text.split("\n").map(l => l.trim()).filter(Boolean);

  for (const line of lines) {
    const upper = line.toUpperCase();

    // Detect section headers
    if (upper.match(/\b(FEEDER|SLAUGHTER|SLTR|BRED|FAT CATTLE|FINISHED|SHEEP|LAMB|GOAT)\b/)) {
      const newType = classifySaleType(line);
      if (newType !== "Other") currentSaleType = newType;
      const d = extractDate(line);
      if (d) currentDate = d;
    }

    // Track column class hints
    if (upper.match(/\bSTEERS?\b/))  currentClass = "STEER";
    if (upper.match(/\bHEIFERS?\b|HFRS\b/)) currentClass = "HEIFER";
    if (upper.match(/\bCOWS?\b/))    currentClass = "COW";
    if (upper.match(/\bBULLS?\b/))   currentClass = "BULL";
    if (upper.match(/\bLAMBS?\b/))   currentClass = "LAMB";

    // Glacial Lakes two-column format: "2 BLK 330 $600.00  6 BLK 340 $600.00"
    const pairPattern = /(\d+)\s+([A-Z]+(?:\/[A-Z]+)?)\s+(\d{2,4})\s+\$?([\d,]+\.?\d*)/gi;
    let pm;
    let found = false;
    while ((pm = pairPattern.exec(line)) !== null) {
      const head   = parseInt(pm[1], 10);
      const color  = pm[2].toUpperCase();
      const weight = parseFloat(pm[3]);
      const price  = parseFloat(pm[4].replace(/,/g, ""));
      if (!head || isNaN(weight) || isNaN(price) || weight < 50 || price < 10) continue;
      lots.push({ head, color, class: currentClass, weight, price,
        sale_type: currentSaleType, sale_date: currentDate || "unknown",
        per_head: isPerHead(currentSaleType, currentClass), barn_id: barnId });
      found = true;
    }

    // Rock Creek representative sales: "Location Color Class Weight Head Price"
    if (!found) {
      const rcm = line.match(/(\d{2,4})\s+(\d+)\s+([\d,]+\.?\d*)$/);
      if (rcm) {
        const weight = parseFloat(rcm[1]);
        const head   = parseInt(rcm[2], 10);
        const price  = parseFloat(rcm[3].replace(/,/g, ""));
        const desc   = line.replace(/(\d{2,4})\s+(\d+)\s+([\d,]+\.?\d*)$/, "").trim();
        const colorM = desc.match(/\b(BLACK|BLK|RED|BWF|RWF|MIXED|CHAR|HOL|XBRED|WHITE|WRF)\b/i);
        const color  = colorM ? colorM[1].toUpperCase().slice(0,3) : "UNK";
        const classM = desc.match(/\b(STEER|HEIFER|COW|BULL|LAMB|EWE|KID|GOAT|SOW|HOG)\b/i);
        const cls    = classM ? classM[1].toUpperCase() : currentClass;
        if (head && !isNaN(weight) && !isNaN(price) && weight > 50 && price > 10) {
          lots.push({ head, color, class: cls, weight, price,
            sale_type: currentSaleType, sale_date: currentDate || "unknown",
            per_head: isPerHead(currentSaleType, cls), barn_id: barnId });
        }
      }
    }
  }

  console.log(`[scraper] PDF (${barnId}): parsed ${lots.length} lots`);
  return lots;
}

// ─── Find latest PDF link from a results page ─────────────────────────────
async function getLatestPDFUrl(resultsPageUrl) {
  const html   = await fetchPage(resultsPageUrl);
  const $      = cheerio.load(html);
  const origin = new URL(resultsPageUrl).origin;
  let pdfUrl   = null;

  $("a[href]").each((_, el) => {
    if (pdfUrl) return;
    const href = $(el).attr("href");
    if (href && href.toLowerCase().includes(".pdf")) {
      pdfUrl = href.startsWith("http") ? href :
               href.startsWith("//")   ? "https:" + href :
               origin + href;
    }
  });

  return pdfUrl;
}

// ─── Per-barn runners ─────────────────────────────────────────────────────
async function scrapeSFRL(barn)       { return parseSFRL(await fetchPage(barn.url), barn.id); }

async function scrapePipestone(barn) {
  const lots = [];
  const saleTypeMap = {
    "https://www.pipestonelivestock.com/feeder-cattle":      "Feeder Cattle",
    "https://www.pipestonelivestock.com/slaughter-fat-bulls": "Slaughter",
    "https://www.pipestonelivestock.com/sheep-goats":         "Sheep & Goats",
  };
  for (const url of barn.urls) {
    try {
      lots.push(...parsePipestone(await fetchPage(url), saleTypeMap[url], barn.id));
    } catch (e) {
      console.warn(`[scraper] Pipestone page failed (${url}): ${e.message}`);
    }
  }
  return lots;
}

async function scrapePDFBarn(barn) {
  const pdfUrl = await getLatestPDFUrl(barn.resultsUrl);
  if (!pdfUrl) { console.warn(`[scraper] No PDF found for ${barn.id}`); return []; }
  const dateHint = extractDate(decodeURIComponent(pdfUrl));
  return parsePDF(await fetchBinary(pdfUrl), barn.id, dateHint);
}

// ─── Save to Supabase ─────────────────────────────────────────────────────
async function saveLots(lots, barnId, sourceUrl) {
  if (!lots.length) { console.warn(`[scraper] No lots to save for ${barnId}`); return; }
  const now  = new Date().toISOString();
  const rows = lots.map(l => ({
    ...l,
    est_value:  l.per_head ? l.head * l.price : Math.round(l.head * l.weight * l.price / 100),
    scraped_at: now,
    source_url: sourceUrl,
  }));

  const { error } = await supabase.from("livestock_lots")
    .upsert(rows, { onConflict: "sale_date,sale_type,class,color,head,weight", ignoreDuplicates: true });

  if (error) { console.error(`[scraper] Supabase error (${barnId}):`, error); throw error; }
  console.log(`[scraper] ✓ Saved ${rows.length} lots for ${barnId}`);

  await supabase.from("scraper_meta").upsert({
    id: barnId, last_scraped: now, lots_count: rows.length, source_url: sourceUrl,
  });
}

// ─── Main ──────────────────────────────────────────────────────────────────
async function runScraper() {
  console.log(`\n[scraper] ===== Run started at ${new Date().toISOString()} =====`);
  const results = [];

  for (const barn of BARNS) {
    console.log(`\n[scraper] --- ${barn.name} ---`);
    try {
      let lots = [];
      if      (barn.parser === "sfrl_html")      lots = await scrapeSFRL(barn);
      else if (barn.parser === "pipestone_html") lots = await scrapePipestone(barn);
      else if (barn.parser === "pdf")            lots = await scrapePDFBarn(barn);
      const sourceUrl = barn.url || barn.resultsUrl || barn.urls?.[0];
      await saveLots(lots, barn.id, sourceUrl);
      results.push({ barn: barn.id, success: true, count: lots.length });
    } catch (err) {
      console.error(`[scraper] ${barn.id} FAILED:`, err.message);
      results.push({ barn: barn.id, success: false, error: err.message });
    }
  }

  console.log("\n[scraper] ===== Summary =====");
  results.forEach(r => console.log(`  ${r.barn}: ${r.success ? `✓ ${r.count} lots` : `✗ ${r.error}`}`));
  console.log("[scraper] ===== Done =====\n");
  return results;
}

module.exports = { runScraper };

if (require.main === module) {
  runScraper().then(() => process.exit(0)).catch(() => process.exit(1));
}
