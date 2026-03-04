// src/scraper.js
// Multi-barn livestock scraper:
//   - SFRL         (HTML tables)
//   - Pipestone    (HTML "N head.....WEIGHT#.....$PRICE" format)
//   - Glacial Lakes (PDF linked from results page)
//   - Rock Creek   (PDF linked from market reports page)

const fetch   = require("node-fetch");
const cheerio = require("cheerio");
const pdf     = require("pdf-parse");
const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

// ─── Barn configs ──────────────────────────────────────────────────────────
const BARNS = [
  {
    id:     "sfrl",
    name:   "Sioux Falls Regional Livestock",
    url:    "https://sfrlinc.com/web/market-reports/",
    parser: "sfrl_html",
  },
  {
    id:     "pipestone",
    name:   "Pipestone Livestock Auction",
    urls: [
      "https://www.pipestonelivestock.com/feeder-cattle",
      "https://www.pipestonelivestock.com/slaughter-fat-bulls",
      "https://www.pipestonelivestock.com/sheep-goats",
    ],
    parser: "pipestone_html",
  },
  {
    id:         "glacial",
    name:       "Glacial Lakes Livestock",
    resultsUrl: "https://glaciallakeslive.com/results",
    parser:     "pdf",
  },
  {
    id:         "rockcreek",
    name:       "Rock Creek Livestock Market",
    resultsUrl: "https://rockcreeklivestockmarket.com/?page_id=348",
    parser:     "pdf",
  },
];

// ─── Shared helpers ────────────────────────────────────────────────────────
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
  if (!res.ok) throw new Error(`HTTP ${res.status} fetching PDF`);
  return res.buffer();
}

function classifySaleType(text) {
  const t = text.toUpperCase();
  if (t.includes("FEEDER"))                                                   return "Feeder Cattle";
  if (t.includes("SLAUGHTER") || t.includes("SLTR") || t.includes("FAT, BULLS")) return "Slaughter";
  if (t.includes("BRED") || t.includes("COW/CALF") || t.includes("C/C"))    return "Bred Cows";
  if (t.includes("FAT CATTLE") || t.includes("FED CATTLE") || t.includes("FINISHED")) return "Fed Cattle";
  if (t.includes("SHEEP") || t.includes("LAMB") || t.includes("GOAT") || t.includes("KID")) return "Sheep & Goats";
  return null;
}

const PER_HEAD_CLASSES = ["BRED COW", "BRED HEIFER", "C/C PAIR", "COW/CALF", "PAIR"];
function isPerHead(saleType, cls) {
  return saleType === "Bred Cows" ||
    PER_HEAD_CLASSES.some(c => cls.toUpperCase().includes(c));
}

// Normalise various date formats to MM/DD/YY
function extractDate(str) {
  // (MM/DD/YY) or (MM/DD/YYYY)
  const m1 = str.match(/\((\d{1,2}\/\d{1,2}\/\d{2,4})\)/);
  if (m1) {
    const p = m1[1].split("/");
    return `${p[0].padStart(2,"0")}/${p[1].padStart(2,"0")}/${p[2].length===4?p[2].slice(2):p[2]}`;
  }
  // M.DD.YY (Glacial Lakes filenames)
  const m2 = str.match(/(\d{1,2})\.(\d{1,2})\.(\d{2,4})/);
  if (m2) {
    const yy = m2[3].length === 4 ? m2[3].slice(2) : m2[3];
    return `${m2[1].padStart(2,"0")}/${m2[2].padStart(2,"0")}/${yy}`;
  }
  // "February 26th 2026" or "March 2nd, 2026"
  const months = {january:"01",february:"02",march:"03",april:"04",may:"05",june:"06",
                  july:"07",august:"08",september:"09",october:"10",november:"11",december:"12"};
  const m3 = str.match(/(\w+)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})/i);
  if (m3) {
    const mon = months[m3[1].toLowerCase()];
    if (mon) return `${mon}/${m3[2].padStart(2,"0")}/${m3[3].slice(2)}`;
  }
  return null;
}

// ─── SFRL HTML parser ──────────────────────────────────────────────────────
function parseSFRL(html, barnId) {
  const $ = cheerio.load(html);
  const lots = [];
  let currentSaleType = "Feeder Cattle";
  let currentDate = null;

  $("h3, h4, table").each((_, el) => {
    const tag  = el.tagName.toLowerCase();
    const text = $(el).text().trim();

    if (tag === "h3" || tag === "h4") {
      const st = classifySaleType(text);
      if (st) currentSaleType = st;
      const d = extractDate(text);
      if (d) currentDate = d;
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

  console.log(`[scraper] SFRL: ${lots.length} lots`);
  return lots;
}

// ─── Pipestone HTML parser ─────────────────────────────────────────────────
// Format: "N head.....WEIGHT#.....$PRICE [COLOR] [class hint]"
// Also handles per-head: "N head.....WEIGHT#.....$PRICE per head"
function parsePipestonePage(html, defaultSaleType, barnId) {
  const $ = cheerio.load(html);
  const lots = [];

  // Find sale date from headings like "February 26th 2026"
  let saleDate = null;
  $("h1,h2,h3,h4,strong,b").each((_, el) => {
    if (!saleDate) { const d = extractDate($(el).text()); if (d) saleDate = d; }
  });

  // Walk all text blocks — results are in <p> or plain text nodes
  const fullText = $("body").text();
  const lines    = fullText.split(/\n/).map(l => l.trim()).filter(Boolean);

  let currentSaleType = defaultSaleType;
  let currentClass    = defaultSaleType === "Slaughter" ? "COW" :
                        defaultSaleType === "Sheep & Goats" ? "LAMB" : "STEER";

  for (const line of lines) {
    // Detect section changes within the page
    const st = classifySaleType(line);
    if (st) { currentSaleType = st; }

    // Class hints
    const upper = line.toUpperCase();
    if (upper.includes("HOLSTEIN") || upper.includes("HOL STEER")) currentClass = "HOL STEER";
    else if (upper.includes("HEIFER"))  currentClass = "HEIFER";
    else if (upper.match(/\bSTEER/))    currentClass = "STEER";
    else if (upper.includes("COW"))     currentClass = "COW";
    else if (upper.includes("BULL"))    currentClass = "BULL";
    else if (upper.includes("LAMB"))    currentClass = "LAMB";
    else if (upper.includes("GOAT") || upper.includes("KID")) currentClass = "GOAT";

    // Match: "N head.....WEIGHT#.....$PRICE" (with optional "per head")
    // e.g. "28 head.....244#.....$1900.00 per head"
    // e.g. "528#.... $427.50 Blk 4 head"  (old format, keep as fallback)
    const m1 = line.match(/(\d+)\s*head\.+(\d+)#\.+\$?([\d,]+\.?\d*)(\s+per\s+head)?/i);
    if (m1) {
      const head    = parseInt(m1[1], 10);
      const weight  = parseFloat(m1[2]);
      const price   = parseFloat(m1[3].replace(/,/g, ""));
      const perHead = !!m1[4] || isPerHead(currentSaleType, currentClass);
      // Extract color if present after the price
      const afterPrice = line.slice(line.indexOf(m1[3]) + m1[3].length).trim();
      const colorM = afterPrice.match(/^(BLK|RED|BWF|RWF|MIXED|CHAR|HOL|XBRED|BLK\/RED|BLK\/BWF)\b/i);
      const color  = colorM ? colorM[1].toUpperCase() : "UNK";
      if (head && weight && price) {
        lots.push({ head, color, class: currentClass, weight, price,
          sale_type: currentSaleType, sale_date: saleDate || "unknown",
          per_head: perHead, barn_id: barnId });
      }
      continue;
    }

    // Fallback: "WEIGHT#.... $PRICE COLOR N head"
    const m2 = line.match(/(\d+)#[.\s]+\$?([\d,]+\.?\d*)\s+(\w+)\s+(\d+)\s+head/i);
    if (m2) {
      const weight = parseFloat(m2[1]);
      const price  = parseFloat(m2[2].replace(/,/g, ""));
      const color  = m2[3].toUpperCase();
      const head   = parseInt(m2[4], 10);
      if (head && weight && price) {
        lots.push({ head, color, class: currentClass, weight, price,
          sale_type: currentSaleType, sale_date: saleDate || "unknown",
          per_head: isPerHead(currentSaleType, currentClass), barn_id: barnId });
      }
    }
  }

  if (lots.length <= 1) {
    console.log(`[scraper] Pipestone DEBUG (${defaultSaleType}) first 30 matching lines:`);
    lines.filter(l => l.match(/head/i)).slice(0,30).forEach((l,i) => console.log(`  ${i}: ${l}`));
  }
  console.log(`[scraper] Pipestone (${defaultSaleType}): ${lots.length} lots`);
  return lots;
}

// ─── PDF parser (Glacial Lakes + Rock Creek) ───────────────────────────────
async function parsePDFBuffer(buffer, barnId, dateHint) {
  const data  = await pdf(buffer);
  const text  = data.text;
  const lots  = [];
  const lines = text.split("\n").map(l => l.trim()).filter(Boolean);

  let currentSaleType = "Feeder Cattle";
  let currentDate     = dateHint || null;
  let currentClass    = "STEER";

  // Rock Creek PDFs have multi-column layout that scrambles when extracted.
  // Instead, scan the raw text for the representative sales blocks which follow
  // a consistent pattern: "LOCATION COLOR CLASS WEIGHT HEAD PRICE"
  if (barnId === "rockcreek") {
    const d = extractDate(text);
    if (d) currentDate = d;
    // Match lines like: "Braham Black Cow 1,945 1 167.50"
    // or "Rush City Black Steer Calves 438 6 483.00"
    // Pattern: text...COLOR CLASS...WEIGHT HEAD PRICE (price has decimal)
    const rcPattern = /([A-Za-z /,&]+?)\s+(Black|Red|Blk|BWF|RWF|Mixed|Char|Hol|Holstein|White|Speckled)[A-Za-z &]*?\s+(Steer|Heifer|Cow|Bull|Lamb|Ewe|Kid|Goat|Sow|Hog|Nanny)[A-Za-z ]*?\s+([\d,]+)\s+(\d+)\s+(\d+\.\d+)/gi;
    let rm;
    while ((rm = rcPattern.exec(text)) !== null) {
      const color  = rm[2].slice(0,3).toUpperCase();
      const cls    = rm[3].toUpperCase();
      const weight = parseFloat(rm[4].replace(/,/g,""));
      const head   = parseInt(rm[5], 10);
      const price  = parseFloat(rm[6]);
      if (!head || weight < 100 || price < 10 || head > 500) continue;
      const saleType = classifySaleType(cls) || classifySaleType(rm[0]) || "Feeder Cattle";
      lots.push({ head, color, class: cls, weight, price,
        sale_type: saleType, sale_date: currentDate || "unknown",
        per_head: isPerHead(saleType, cls), barn_id: barnId });
    }
    // Also match per-head bred cattle: "Location Color Cows Bred X Mo WEIGHT HEAD PRICE"
    const brPattern = /(Black|Red|Blk|BWF|RWF|Mixed)[A-Za-z /]*?(Cow|Heifer)[A-Za-z /]*?Bred[A-Za-z \d/]*([\d,]+)\s+(\d+)\s+(\d[\d,]+\.\d+)/gi;
    let bm;
    while ((bm = brPattern.exec(text)) !== null) {
      const color  = bm[1].slice(0,3).toUpperCase();
      const cls    = "BRED " + bm[2].toUpperCase();
      const weight = parseFloat(bm[3].replace(/,/g,""));
      const head   = parseInt(bm[4], 10);
      const price  = parseFloat(bm[5].replace(/,/g,""));
      if (!head || weight < 500 || price < 100) continue;
      lots.push({ head, color, class: cls, weight, price,
        sale_type: "Bred Cows", sale_date: currentDate || "unknown",
        per_head: true, barn_id: barnId });
    }
    if (lots.length === 0) {
      console.log(`[scraper] PDF DEBUG (${barnId}) first 40 lines:`);
      lines.slice(0,40).forEach((l,i) => console.log(`  ${i}: ${l}`));
    }
    console.log(`[scraper] PDF (${barnId}): ${lots.length} lots`);
    return lots;
  }

  for (const line of lines) {
    const upper = line.toUpperCase();

    // Section headers
    const st = classifySaleType(line);
    if (st) currentSaleType = st;
    const d = extractDate(line);
    if (d) currentDate = d;

    // Class column headers
    if (upper.match(/\bSTEERS?\b/) && !upper.match(/\d/)) currentClass = "STEER";
    if (upper.match(/\bHEIFERS?\b|HFRS\b/) && !upper.match(/\d/)) currentClass = "HEIFER";
    if (upper.match(/\bCOWS?\b/) && !upper.match(/\d/))   currentClass = "COW";
    if (upper.match(/\bBULLS?\b/) && !upper.match(/\d/))  currentClass = "BULL";
    if (upper.match(/\bLAMBS?\b/) && !upper.match(/\d/))  currentClass = "LAMB";
    if (upper.match(/\bEWES?\b/)  && !upper.match(/\d/))  currentClass = "EWE";

    // ── Glacial Lakes two-column format ──
    // "2 BLK 330 $600.00  6 BLK 340 $600.00"
    // each column: HEAD COLOR WEIGHT $PRICE
    // Glacial Lakes: "2 BLK330$600.006 BLK340$600.00"
    // HEAD(space)COLOR(no space)WEIGHT$PRICE — repeated twice per line
    const glPattern = /(\d+)\s+([A-Z]+(?:\/[A-Z]+)?)(\d{3,4})\$([0-9]+\.[0-9]+)/g;
    let gm;
    let foundGL = false;
    while ((gm = glPattern.exec(line)) !== null) {
      const head   = parseInt(gm[1], 10);
      const color  = gm[2].toUpperCase();
      const weight = parseFloat(gm[3]);
      const price  = parseFloat(gm[4]);
      if (!head || weight < 50 || price < 5) continue;
      lots.push({ head, color, class: currentClass, weight, price,
        sale_type: currentSaleType, sale_date: currentDate || "unknown",
        per_head: isPerHead(currentSaleType, currentClass), barn_id: barnId });
      foundGL = true;
    }
    if (foundGL) continue;

    // Rock Creek: "Braham Black Cow 1,945 1 167.50"
    // Skip range lines like "300-400 lbs 400.00 610.00"
    if (!line.match(/\d+-\d+\s+lbs/i)) {
      const rcm = line.match(/([\d,]+)\s+(\d+)\s+([\d,]+\.\d+)$/);
      if (rcm) {
        const weight = parseFloat(rcm[1].replace(/,/g, ""));
        const head   = parseInt(rcm[2], 10);
        const price  = parseFloat(rcm[3].replace(/,/g, ""));
        if (head && !isNaN(weight) && !isNaN(price) && weight > 100 && price > 10 && head <= 500) {
          const desc   = line.slice(0, line.lastIndexOf(rcm[0])).trim();
          const colorM = desc.match(/\b(BLACK|BLK|RED|BWF|RWF|MIXED|CHAR|HOL|XBRED|WHITE|WRF|SPECKLED)\b/i);
          const color  = colorM ? colorM[1].slice(0,3).toUpperCase() : "UNK";
          const classM = desc.match(/\b(STEER|HEIFER|COW|BULL|LAMB|EWE|KID|GOAT|SOW|HOG)\b/i);
          const cls    = classM ? classM[1].toUpperCase() : currentClass;
          lots.push({ head, color, class: cls, weight, price,
            sale_type: currentSaleType, sale_date: currentDate || "unknown",
            per_head: isPerHead(currentSaleType, cls), barn_id: barnId });
        }
      }
    }
  }

  if (lots.length === 0) {
    console.log(`[scraper] PDF DEBUG (${barnId}) first 40 lines:`);
    lines.slice(0,40).forEach((l,i) => console.log(`  ${i}: ${l}`));
  }
  console.log(`[scraper] PDF (${barnId}): ${lots.length} lots`);
  return lots;
}

// ─── Find latest PDF link from a results page ──────────────────────────────
async function getLatestPDFUrl(resultsPageUrl) {
  const html   = await fetchPage(resultsPageUrl);
  const $      = cheerio.load(html);
  const origin = new URL(resultsPageUrl).origin;
  let pdfUrl   = null;

  $("a[href]").each((_, el) => {
    if (pdfUrl) return;
    const href = $(el).attr("href") || "";
    if (href.toLowerCase().includes(".pdf")) {
      pdfUrl = href.startsWith("http") ? href :
               href.startsWith("//")   ? "https:" + href :
               origin + href;
    }
  });

  if (pdfUrl) console.log(`[scraper] Found PDF: ${pdfUrl}`);
  else        console.warn(`[scraper] No PDF link found on ${resultsPageUrl}`);
  return pdfUrl;
}

// ─── Per-barn runners ──────────────────────────────────────────────────────
async function scrapeSFRL(barn) {
  return parseSFRL(await fetchPage(barn.url), barn.id);
}

async function scrapePipestone(barn) {
  const saleTypeMap = {
    "https://www.pipestonelivestock.com/feeder-cattle":       "Feeder Cattle",
    "https://www.pipestonelivestock.com/slaughter-fat-bulls": "Slaughter",
    "https://www.pipestonelivestock.com/sheep-goats":         "Sheep & Goats",
  };
  const lots = [];
  for (const url of barn.urls) {
    try {
      lots.push(...parsePipestonePage(await fetchPage(url), saleTypeMap[url], barn.id));
    } catch (e) {
      console.warn(`[scraper] Pipestone page failed (${url}): ${e.message}`);
    }
  }
  return lots;
}

async function scrapePDFBarn(barn) {
  const pdfUrl = await getLatestPDFUrl(barn.resultsUrl);
  if (!pdfUrl) return [];
  const dateHint = extractDate(decodeURIComponent(pdfUrl));
  return parsePDFBuffer(await fetchBinary(pdfUrl), barn.id, dateHint);
}

// ─── Save to Supabase ──────────────────────────────────────────────────────
async function saveLots(lots, barnId, sourceUrl) {
  if (!lots.length) { console.warn(`[scraper] No lots for ${barnId}`); return; }
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
  console.log(`\n[scraper] ===== Run started ${new Date().toISOString()} =====`);
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
