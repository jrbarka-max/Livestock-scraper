// src/index.js
// Runs the scraper on a weekly cron schedule.
// Also exposes a tiny HTTP server so Railway keeps the process alive
// and you can trigger a manual scrape via POST /scrape.

const http   = require("http");
const cron   = require("node-cron");
const { runScraper } = require("./scraper");

// ─── Schedule ─────────────────────────────────────────────────────────────
// SFRL runs sales on Tuesdays, Wednesdays, Fridays.
// We scrape every Wednesday at 8 PM CT (01:00 UTC Thursday) to catch Tuesday results,
// and every Saturday at 6 AM CT (11:00 UTC) to catch Friday results.
// Adjust these if the barn changes their sale days.

const SCHEDULES = [
  { label: "Wednesday 8pm CT (Tues results)",  cron: "0 1 * * 4"  },  // Thu 01:00 UTC
  { label: "Saturday 6am CT (Fri results)",    cron: "0 11 * * 6" },  // Sat 11:00 UTC
];

SCHEDULES.forEach(({ label, cron: expr }) => {
  cron.schedule(expr, async () => {
    console.log(`\n[cron] Triggered: ${label}`);
    await runScraper();
  });
  console.log(`[cron] Scheduled: ${label} (${expr})`);
});

// ─── HTTP server ──────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;

const server = http.createServer(async (req, res) => {
  // Health check
  if (req.method === "GET" && req.url === "/") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ status: "ok", service: "livestock-scraper", time: new Date().toISOString() }));
    return;
  }

  // Manual scrape trigger (protect with a simple token)
  if (req.method === "POST" && req.url === "/scrape") {
    const authHeader = req.headers["authorization"] || "";
    const token      = process.env.SCRAPE_SECRET || "";

    if (token && authHeader !== `Bearer ${token}`) {
      res.writeHead(401, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "Unauthorized" }));
      return;
    }

    res.writeHead(202, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ message: "Scrape started" }));

    // Run async after responding so the HTTP request doesn't time out
    runScraper().then((result) => {
      console.log("[manual trigger] result:", result);
    });
    return;
  }

  res.writeHead(404);
  res.end("Not found");
});

server.listen(PORT, () => {
  console.log(`[server] Listening on port ${PORT}`);
  console.log(`[server] POST /scrape to trigger manually (set SCRAPE_SECRET env var)`);
});
