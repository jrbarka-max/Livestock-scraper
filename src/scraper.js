// src/scraper.js
// Fetches the SFRL market report page, parses it with cheerio (free, no AI),
// and upserts results into Supabase.

const fetch    = require("node-fetch");
const cheerio  = require("cheerio");
const { createClient } = require("@supabase/supabase-js");

const SFRL_URL = "https://sfrlinc.com/web/market-reports/";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

// ─── Step 1: Fetch the page ────────────────────────────────────────────────
async function fetchPage(url) {
  console.log(`[scraper] Fetching ${url}`);
  const res = await fetch(url, {
    headers: { "User-Agent": "Mozilla/5.0 (compatible; LivestockScraper/1.0)" },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} fetching ${url}`);
  return res.text();
}

// ─── Step 2: Classify a section header into a sale type ───────────────────
function classifySaleType(headerText) {
  const t = headerText.toUpperCase();
  if (t.includes("FEEDER"))   return "Feeder Cattle";
  if (t.includes("SLAUGHTER")) return "Slaughter";
  if (t.includes("BRED") || t.includes("COW/CALF") || t.includes("C/C")) return "Bred Cows";
  if (t.includes("FED") || t.includes("FAT"))  return "Fed Cattle";
  return "Other";
}

// Classes that are always priced per head (not per cwt)
const PER_HEAD_CLASSES = ["BRED COW", "BRED HEIFER", "C/C PAIR", "COW/CALF PAIR"];

function isPerHead(saleType, className) {
  return saleType === "Bred Cows" ||
    PER_HEAD_CLASSES.some(c => className.toUpperCase().includes(c));
}

// Extract MM/DD/YY date from a string like "FEEDER CATTLE (02/23/26) – RESULTS"
function extractDate(str) {
  const m = str.match(/\((\d{2}\/\d{2}\/\d{2,4})\)/);
  if (!m) return null;
  // Normalise to MM/DD/YY
  const parts = m[1].split("/");
  const yy = parts[2].length === 4 ? parts[2].slice(2) : parts[2];
  return `${parts[0]}/${parts[1]}/${yy}`;
}

// ─── Step 3: Parse HTML with cheerio ──────────────────────────────────────
function parseHTML(html) {
  const $ = cheerio.load(html);
  const lots = [];

  let currentSaleType = "Feeder Cattle";
  let currentDate     = null;

  // Walk every element inside the main content area in document order
  $("h3, h4, table").each((_, el) => {
    const tag  = el.tagName.toLowerCase();
    const text = $(el).text().trim();

    // Section headers update the current sale type + date
    if (tag === "h3" || tag === "h4") {
      const upper = text.toUpperCase();
      if (
        upper.includes("FEEDER") || upper.includes("SLAUGHTER") ||
        upper.includes("BRED")   || upper.includes("FED CATTLE") ||
        upper.includes("FAT")
      ) {
        currentSaleType = classifySaleType(text);
        const d = extractDate(text);
        if (d) currentDate = d;
      }
      return;
    }

    // Parse table rows — each row is one lot
    $(el).find("tr").each((_, row) => {
      const cells = $(row).find("td").map((_, td) => $(td).text().trim()).get();

      // SFRL tables have 5 columns: head | color | class | weight | price
      // Skip rows with fewer than 5 cells or non-numeric first cell
      if (cells.length < 5) return;

      const head   = parseInt(cells[0], 10);
      const color  = cells[1].toUpperCase().trim();
      const cls    = cells[2].toUpperCase().trim();
      const weight = parseFloat(cells[3].replace(/,/g, ""));
      const price  = parseFloat(cells[4].replace(/[$,]/g, ""));

      if (!head || isNaN(weight) || isNaN(price) || !cls) return;

      const perHead = isPerHead(currentSaleType, cls);

      lots.push({
        head,
        color,
        class:     cls,
        weight,
        price,
        sale_type: currentSaleType,
        sale_date: currentDate || "unknown",
        per_head:  perHead,
      });
    });
  });

  console.log(`[scraper] Parsed ${lots.length} lots from HTML`);
  return lots;
}

// ─── Step 4: Upsert into Supabase ─────────────────────────────────────────
async function saveLots(lots) {
  if (!lots.length) {
    console.warn("[scraper] No lots to save.");
    return;
  }

  // Add a computed est_value and scraped_at timestamp
  const now = new Date().toISOString();
  const rows = lots.map((l) => ({
    ...l,
    est_value: l.per_head
      ? l.head * l.price
      : Math.round(l.head * l.weight * l.price / 100),
    scraped_at: now,
    source_url: SFRL_URL,
  }));

  // Upsert — unique on (sale_date, sale_type, class, color, head, weight)
  // so re-running doesn't duplicate data
  const { error, count } = await supabase
    .from("livestock_lots")
    .upsert(rows, {
      onConflict: "sale_date,sale_type,class,color,head,weight",
      ignoreDuplicates: true,
    });

  if (error) {
    console.error("[scraper] Supabase upsert error:", error);
    throw error;
  }

  console.log(`[scraper] ✓ Saved ${rows.length} lots to Supabase`);

  // Also update the "last_scraped" metadata row
  await supabase.from("scraper_meta").upsert({
    id: "sfrl",
    last_scraped: now,
    lots_count: rows.length,
    source_url: SFRL_URL,
  });
}

// ─── Main ──────────────────────────────────────────────────────────────────
async function runScraper() {
  console.log(`\n[scraper] ===== Run started at ${new Date().toISOString()} =====`);
  try {
    const html  = await fetchPage(SFRL_URL);
    const lots  = parseHTML(html);
    await saveLots(lots);
    console.log("[scraper] ===== Done ✓ =====\n");
    return { success: true, count: lots.length };
  } catch (err) {
    console.error("[scraper] ===== FAILED =====", err.message);
    return { success: false, error: err.message };
  }
}

module.exports = { runScraper };

// Allow direct execution: `node src/scraper.js`
if (require.main === module) {
  runScraper().then((r) => {
    process.exit(r.success ? 0 : 1);
  });
}
