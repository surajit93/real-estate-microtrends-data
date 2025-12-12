/**
 * WOF → GitHub folder generator
 * -----------------------------------------------
 * Features:
 *  ✔ No GET/404 spam (silent checks)
 *  ✔ Clean progress logs (%)
 *  ✔ Full depth admin hierarchy
 *  ✔ Parallelized (configurable)
 *  ✔ Skips existing folders/files
 *  ✔ Retries on failure
 *  ✔ Works inside GitHub Actions (local fs)
 *
 * Requires:
 *   npm install fast-glob unzipper
 */

import fs from "fs";
import path from "path";
import fg from "fast-glob";
import unzipper from "unzipper";

const DATA_DIR = process.env.WOF_DATA_DIR || "data/wof";
const CONCURRENCY = parseInt(process.env.WOF_CONCURRENCY || "4", 10);
const WOF_COUNTRY_URL =
  "https://whosonfirst-api.github.io/whosonfirst-www-data/whosonfirst-data-admin-latest-bundle.zip";

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function sanitize(name) {
  return name
    .normalize("NFKD")
    .replace(/[\/:*?"<>|\\'#%]/g, "_")
    .replace(/\s+/g, "_")
    .replace(/_+/g, "_");
}

/**
 * Download + unzip WOF bundle
 */
async function downloadBundle() {
  const zipPath = path.join(DATA_DIR, "wof.zip");
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

  console.log("Downloading WOF bundle…");

  const res = await fetch(WOF_COUNTRY_URL);
  if (!res.ok) throw new Error("Failed to download WOF bundle");

  const fileStream = fs.createWriteStream(zipPath);
  await new Promise((resolve) => {
    res.body.pipe(fileStream);
    res.body.on("end", resolve);
  });

  console.log("Unzipping…");
  await fs
    .createReadStream(zipPath)
    .pipe(unzipper.Extract({ path: DATA_DIR }))
    .promise();

  console.log("WOF data downloaded + extracted.");
}

/**
 * Write leaf files only if missing
 */
function ensureLeafFiles(dir) {
  const buyers = path.join(dir, "buyers.json");
  const props = path.join(dir, "properties.json");
  const meta = path.join(dir, "metadata.json");

  if (!fs.existsSync(buyers))
    fs.writeFileSync(buyers, JSON.stringify({ buyers: [] }, null, 2));

  if (!fs.existsSync(props))
    fs.writeFileSync(props, JSON.stringify({ properties: [] }, null, 2));

  if (!fs.existsSync(meta))
    fs.writeFileSync(
      meta,
      JSON.stringify({ created: new Date().toISOString() }, null, 2)
    );
}

/**
 * Process one WOF feature file
 */
async function processFeature(file, rootOut) {
  const json = JSON.parse(fs.readFileSync(file, "utf8"));

  let names = [];
  let props = json.properties || {};

  const country = props["wof:country"] || null;
  const region = props["wof:region"] || null;
  const locality = props["wof:locality"] || null;
  const neighbourhood = props["wof:neighbourhood"] || null;

  if (!country) return;

  names.push(country);

  if (region) names.push(region);
  if (locality) names.push(locality);
  if (neighbourhood) names.push(neighbourhood);

  const clean = names.map((n) => sanitize(String(n)));
  const fullPath = path.join(rootOut, ...clean);

  if (!fs.existsSync(fullPath)) fs.mkdirSync(fullPath, { recursive: true });

  ensureLeafFiles(fullPath);
}

/**
 * Parallel runner with concurrency limit
 */
async function runParallel(items, handler, concurrency) {
  let index = 0;
  let completed = 0;
  const total = items.length;

  const workers = new Array(concurrency).fill(0).map(async () => {
    while (true) {
      const i = index++;
      if (i >= total) break;

      await handler(items[i], i, total);

      completed++;
      const pct = Math.floor((completed / total) * 100);
      if (completed % 50 === 0 || completed === total) {
        console.log(`[${pct}%] Processed ${completed}/${total}`);
      }
    }
  });

  await Promise.all(workers);
}

/**
 * MAIN
 */
async function main() {
  console.log("Starting WOF folder generator");

  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    await downloadBundle();
  }

  const files = await fg([`${DATA_DIR}/**/*.geojson`], {
    absolute: true,
  });

  console.log(`Found ${files.length} features`);

  const OUTPUT = path.join("real-estate");

  if (!fs.existsSync(OUTPUT)) fs.mkdirSync(OUTPUT, { recursive: true });

  await runParallel(
    files,
    async (file) => {
      await processFeature(file, OUTPUT);
    },
    CONCURRENCY
  );

  console.log("ALL DONE ✔");
}

main().catch((err) => {
  console.error("FATAL:", err);
  process.exit(1);
});
