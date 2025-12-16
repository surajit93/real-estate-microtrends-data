// ==========================================================
// wof_build_tree_v7.js — FIXED, FULLY WORKING
// ==========================================================

import fs from "fs";
import fsp from "fs/promises";
import path from "path";
import { execSync } from "child_process";
import { Octokit } from "@octokit/rest";

const TOKEN = process.env.PAT_TOKEN;
if (!TOKEN) throw new Error("PAT_TOKEN missing");

const octokit = new Octokit({ auth: TOKEN });

// ------------------------------------------------------------
// PROGRESS
// ------------------------------------------------------------
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

const WOF_ORG = "whosonfirst-data";
const ROOT = "geolocations";

if (!fs.existsSync(ROOT)) fs.mkdirSync(ROOT, { recursive: true });

// ------------------------------------------------------------
// HELPERS
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

async function ensureLeaf(folder) {
  const re = path.join(folder, "real-estate");
  await ensureDir(re);

  await fsp.writeFile(path.join(re, "metadata.json"), JSON.stringify({ created: new Date().toISOString() }, null, 2));
  await fsp.writeFile(path.join(re, "properties.json"), JSON.stringify({ properties: [] }, null, 2));
  await fsp.writeFile(path.join(re, "buyers.json"), JSON.stringify({ buyers: [] }, null, 2));
}

function parseRecord(raw) {
  const p = raw.properties || {};
  const pt = p["wof:placetype"];

  if (!["country","region","county","locality","neighbourhood","microhood"].includes(pt))
    return null;

  return {
    id: String(p["wof:id"]),
    name: p["wof:name"] || null,
    placetype: pt,
    parent: String(p["wof:parent_id"] || ""),
    children: []
  };
}

function buildTree(records) {
  const map = new Map();
  records.forEach(r => map.set(r.id, r));
  records.forEach(r => {
    const pr = map.get(r.parent);
    if (pr) pr.children.push(r);
  });
  return [...map.values()].filter(n => n.placetype === "country");
}

async function createTree(node, base) {
  const folder = path.join(base, sanitize(node.name));
  await ensureDir(folder);

  if (!node.children.length) {
    await ensureLeaf(folder);
    return;
  }

  for (const ch of node.children) {
    await createTree(ch, folder);
  }
}

// ------------------------------------------------------------
// PROCESS ONE COUNTRY REPO
// ------------------------------------------------------------
async function processRepo(repo, index) {
  const name = repo.name;
  const code = name.replace("whosonfirst-data-admin-", "").toUpperCase();

  if (!ALLOWED.has(code)) return;

  console.log("Processing country:", code);

  const cloneDir = `_wof_cache/${name}`;
  if (!fs.existsSync("_wof_cache")) fs.mkdirSync("_wof_cache");

  if (!fs.existsSync(cloneDir)) {
    console.log("Cloning:", name);
    execSync(`git clone --depth=1 ${repo.clone_url} ${cloneDir}`, { stdio: "ignore" });
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

  for (const file of files) {
    const raw = JSON.parse(fs.readFileSync(file, "utf8"));
    const rec = parseRecord(raw);
    if (rec) parsed.push(rec);
  }

  if (!parsed.length) return;

  const roots = buildTree(parsed);

  const countryBase = path.join(ROOT, code);
  await ensureDir(countryBase);

  for (const root of roots) {
    await createTree(root, countryBase);
  }

  progress.repo_index = index + 1;
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));
}

// ------------------------------------------------------------
// MAIN
// ------------------------------------------------------------
(async function main() {
  console.log("Hybrid V7 started — FIXED REPO FETCH");

  const repos = await octokit.paginate(octokit.repos.listForOrg, {
    org: WOF_ORG,
    per_page: 100
  });

  const adminRepos = repos
    .filter(r => r.name.startsWith("whosonfirst-data-admin-"))
    .sort((a, b) => a.name.localeCompare(b.name));

  for (let i = progress.repo_index; i < adminRepos.length; i++) {
    await processRepo(adminRepos[i], i);
  }

  console.log("DONE — full geolocation tree generated");
})();
