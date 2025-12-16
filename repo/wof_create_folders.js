// ==========================================================
// wof_build_tree_v6.js  — FULL WORKING VERSION
// ==========================================================
// • Downloads full WOF admin repos locally via git clone
// • Processes ONLY your requested countries
// • Builds folder tree in ./geolocations
// • Leaf → creates ./real-estate metadata/buyers/properties
// • Tracks progress in .real_estate_progress/.progress.json
// • Commits the tree in one push (GitHub workflow step)
// ==========================================================

import fs from "fs";
import fsp from "fs/promises";
import path from "path";
import { execSync } from "child_process";

const PROGRESS_DIR = ".real_estate_progress";
const PROGRESS_FILE = path.join(PROGRESS_DIR, ".progress.json");

if (!fs.existsSync(PROGRESS_DIR)) fs.mkdirSync(PROGRESS_DIR);
if (!fs.existsSync(PROGRESS_FILE)) {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify({ repo_index: 0 }, null, 2));
}

let progress = JSON.parse(fs.readFileSync(PROGRESS_FILE, "utf8"));

// ------------------------------------------------------------
// COUNTRY LIST (YOUR EXACT LIST)
// ------------------------------------------------------------
const ALLOWED = new Set([
  "IN","PK","NP","BD","TH","LK","VN","KH","LA","AU","JP","RU","GB","MX","CA",
  "BR","PE","CO","AR","CL","IT","FR","DE","BE","NL","ES","GR","EG","KE","TZ",
  "ZA","SA","AE","QA","KW","BH","OM","CN","TW","KR","KP","NZ","ID","MY","SG","PH"
]);

const ROOT = "geolocations";
if (!fs.existsSync(ROOT)) fs.mkdirSync(ROOT, { recursive: true });

const WOF_ORG = "whosonfirst-data";

// ------------------------------------------------------------
// Helper functions
// ------------------------------------------------------------

function sanitize(s) {
  return s
    .normalize("NFKD")
    .replace(/[^\w\s-]/g, "")
    .replace(/\s+/g, "_")
    .trim();
}

async function ensureDir(p) {
  await fsp.mkdir(p, { recursive: true });
}

async function ensureLeafRealEstate(folder) {
  const re = path.join(folder, "real-estate");
  await ensureDir(re);

  await fsp.writeFile(path.join(re, "metadata.json"),
    JSON.stringify({ created: new Date().toISOString() }, null, 2));
  await fsp.writeFile(path.join(re, "properties.json"),
    JSON.stringify({ properties: [] }, null, 2));
  await fsp.writeFile(path.join(re, "buyers.json"),
    JSON.stringify({ buyers: [] }, null, 2));
}

// ------------------------------------------------------------
// Parse WOF .geojson record
// ------------------------------------------------------------
function parseRecord(obj) {
  const p = obj.properties || {};
  const pt = p["wof:placetype"];

  if (!["country","region","locality","neighbourhood","microhood","county"].includes(pt))
    return null;

  return {
    id: String(p["wof:id"]),
    name: p["wof:name"] || null,
    placetype: pt,
    parent: String(p["wof:parent_id"] || ""),
    children: []
  };
}

// ------------------------------------------------------------
// Build in-memory hierarchy
// ------------------------------------------------------------
function buildTree(records) {
  const map = new Map();
  records.forEach(r => map.set(r.id, r));
  records.forEach(r => {
    const pr = map.get(r.parent);
    if (pr) pr.children.push(r);
  });
  return [...map.values()].filter(n => n.placetype === "country");
}

// ------------------------------------------------------------
// Recursively create folders
// ------------------------------------------------------------
async function createTree(node, basePath) {
  const folder = path.join(basePath, sanitize(node.name));
  await ensureDir(folder);

  if (!node.children.length) {
    await ensureLeafRealEstate(folder);
    return;
  }

  for (const ch of node.children) {
    await createTree(ch, folder);
  }
}

// ------------------------------------------------------------
// PROCESS ONE COUNTRY REPO
// ------------------------------------------------------------
async function processRepo(repoName, repoIndex) {
  const code = repoName.replace("whosonfirst-data-admin-", "").toUpperCase();

  if (!ALLOWED.has(code)) return;

  console.log("Processing country:", code);

  const cloneDir = `./_wof_cache/${repoName}`;
  if (!fs.existsSync("_wof_cache")) fs.mkdirSync("_wof_cache");

  if (!fs.existsSync(cloneDir)) {
    console.log("Cloning:", repoName);
    execSync(`git clone --depth=1 https://github.com/${WOF_ORG}/${repoName}.git ${cloneDir}`, { stdio: "ignore" });
  }

  const files = [];
  function scan(dir) {
    for (const f of fs.readdirSync(dir)) {
      const full = path.join(dir, f);
      if (fs.statSync(full).isDirectory()) scan(full);
      else if (full.endsWith(".geojson")) files.push(full);
    }
  }
  scan(path.join(cloneDir, "data"));

  const parsed = [];

  files.forEach((file, idx) => {
    progress.repo_index = repoIndex;
    fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));

    try {
      const raw = JSON.parse(fs.readFileSync(file, "utf8"));
      const rec = parseRecord(raw);
      if (rec) parsed.push(rec);
    } catch {}
  });

  if (!parsed.length) return;

  const roots = buildTree(parsed);

  const countryBase = path.join(ROOT, code);
  await ensureDir(countryBase);

  for (const root of roots) {
    await createTree(root, countryBase);
  }
}

// ------------------------------------------------------------
// MAIN
// ------------------------------------------------------------
(async function main() {
  console.log("Hybrid V6 started");

  // get list of WOF admin repos via GitHub API (lightweight)
  const repos = await (await fetch(`https://api.github.com/orgs/${WOF_ORG}/repos?per_page=200`)).json();
  const adminRepos = repos
    .filter(r => r.name.startsWith("whosonfirst-data-admin-"))
    .sort((a,b) => a.name.localeCompare(b.name));

  for (let i = progress.repo_index; i < adminRepos.length; i++) {
    const repoObj = adminRepos[i];
    try {
      await processRepo(repoObj.name, i);
    } catch (e) {
      console.log("Failed repo:", repoObj.name, e.message);
    }
  }

  console.log("DONE — LOCAL TREE COMPLETE");
})();
