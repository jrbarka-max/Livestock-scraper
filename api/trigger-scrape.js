// api/trigger-scrape.js
// Vercel serverless function — proxies workflow_dispatch to GitHub Actions.
// Keeps the GitHub token server-side so it's never exposed in the browser.
//
// Add to your livestock-dashboard repo as: api/trigger-scrape.js
// Add to Vercel environment variables: GITHUB_TOKEN (fine-grained, Actions write)

export default async function handler(req, res) {
  // Only allow POST
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  const token = process.env.GITHUB_TOKEN;
  if (!token) {
    return res.status(500).json({ error: "GITHUB_TOKEN not configured" });
  }

  try {
    const response = await fetch(
      "https://api.github.com/repos/jrbarka-max/Livestock-scraper/actions/workflows/scrape.yml/dispatches",
      {
        method: "POST",
        headers: {
          "Authorization": `Bearer ${token}`,
          "Accept": "application/vnd.github+json",
          "Content-Type": "application/json",
          "X-GitHub-Api-Version": "2022-11-28",
        },
        body: JSON.stringify({ ref: "main" }),
      }
    );

    if (!response.ok) {
      const text = await response.text();
      return res.status(response.status).json({ error: text });
    }

    return res.status(200).json({ message: "Scrape started" });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
}
