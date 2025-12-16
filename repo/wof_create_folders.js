// ==========================================================
// wof_build_tree_v4.js  (FINAL VERSION — BUILD LOCALLY, NO API PUTS)
// ==========================================================
// Builds folder tree under ./geolocations/... using WOF GitHub repos.
// Only your requested countries are processed.
// `.progress.json` tracks last completed repo & file.
// After tree is built, GitHub Action commits everything in one go.
// ==========================================================

import fs from "fs";
import fsp from "fs/promises";
import path from "path";
import axios from "axios";
import { Octokit } from "@octokit/rest";

const TOKEN = process.env.PAT_TOKEN;
if (!TOKEN) throw new Error("PAT_TOKEN missing");

const octokit = new Octokit({ auth: TOKEN });

// -----------------------------------------------
// COUNTRY ISO2 LIST (YOUR EXACT LIST)
// -----------------------------------------------
const ALLOWED = new Set([
  "IN","PK","NP","BD","TH","LK","VN","KH","LA","AU","JP","RU","GB","MX","CA",
  "BR","PE","CO","AR","CL","IT","FR","DE","BE","NL","ES","GR","EG","KE","TZ",
  "ZA","SA","AE","QA","KW","BH","OM","CN","TW","KR","KP","NZ","ID","MY","SG","PH"
]);

// WOF admin repos look like: whosonfirst-data-admin-<code>
// e.g. whosonfirst-data-admin-in → India
const WOF_ORG = "whosonfirst-data";

// -----------------------------------------------
const ROOT = "geolocations";
if (!fs.existsSync(ROOT)) fs.mkdirSync(ROOT, { recursive: true });

const PROGRESS_FILE = ".progress.json";

let progress = { repo_index: 0, file_index: 0 };
if (fs.existsSync(PROGRESS_FILE)) {
  try { progress = JSON.parse(fs.readFileSync(PROGRESS_FILE, "utf8")); }
  catch {}
}

function saveProgress() {
  fs.writeFileSync(PROGRESS_FILE, JSON.stringify(progress, null, 2));
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function sanitize(s) {
  return s
    .normalize("NFKD")
    .replace(/[^\w\s-]/g, "")
    .replace(/\s+/g, "_")
    .trim();
}

// -----------------------------------------------
// Fetch all admin repos in WOF org
// -----------------------------------------------
async function listAdminRepos() {
  return await octokit.paginate(octokit.repos.listForOrg, {
    org: WOF_ORG,
    per_page: 100
  });
}

// -----------------------------------------------
// Fetch repo tree (list of all paths)
// -----------------------------------------------
async function fetchRepoTree(owner, repo) {
  const meta = await octokit.repos.get({ owner, repo });
  const branch = meta.data.default_branch;

  const tree = await octokit.git.getTree({
    owner,
    repo,
    tree_sha: branch,
    recursive: "1"
  });

  return tree.data.tree
    .map(t => t.path)
    .filter(p => p.endsWith(".geojson"));
}

// -----------------------------------------------
// Fetch raw file from repo
// -----------------------------------------------
async function fetchRaw(owner, repo, p) {
  const url = `https://raw.githubusercontent.com/${owner}/${repo}/main/${p}`;
  try {
    const r = await axios.get(url, { timeout: 60000 });
    return r.data;
  } catch {
    return null;
  }
}

// -----------------------------------------------
// Parse WOF record
// -----------------------------------------------
function parseRecord(obj) {
  const p = obj.properties || {};
  const pt = p["wof:placetype"];

  // allow: country, region, locality, neighbourhood, microhood
  if (!["country","region","locality","neighbourhood","microhood"].includes(pt))
    return null;

  return {
    id: String(p["wof:id"]),
    name: p["wof:name"] || null,
    placetype: pt,
    parent: String(p["wof:parent_id"] || ""),
    children: []
  };
}

// -----------------------------------------------
// Build hierarchy tree per repo
// -----------------------------------------------
function buildTree(records) {
  const map = new Map();
  records.forEach(r => map.set(r.id, r));
  records.forEach(r => {
    const parent = map.get(r.parent);
    if (parent) parent.children.push(r);
  });
  return [...map.values()].filter(r => r.placetype === "country");
}

// -----------------------------------------------
// CREATE FOLDER STRUCTURE LOCALLY
// -----------------------------------------------
async function ensureDir(p) {
  await fsp.mkdir(p, { recursive: true });
}

async function ensureLeafRealEstate(p) {
  const re = path.join(p, "real-estate");
  await ensureDir(re);
  await fsp.writeFile(path.join(re, "metadata.json"), JSON.stringify({ created: new Date().toISOString() }, null, 2));
  await fsp.writeFile(path.join(re, "properties.json"), JSON.stringify({ properties: [] }, null, 2));
  await fsp.writeFile(path.join(re, "buyers.json"), JSON.stringify({ buyers: [] }, null, 2));
}

async function createTree(node, basePath) {
  const p = path.join(basePath, sanitize(node.name));
  await ensureDir(p);

  if (!node.children.length) {
    await ensureLeafRealEstate(p);
    return;
  }

  for (const ch of node.children) {
    await createTree(ch, p);
  }
}

// -----------------------------------------------
// PROCESS REPO
// -----------------------------------------------
async function processRepo(repoObj, repoIndex) {
  const name = repoObj.name;
  const code = name.replace("whosonfirst-data-admin-", "").toUpperCase();

  if (!ALLOWED.has(code)) return; // skip unwanted countries

  console.log("Processing:", name);

  const files = await fetchRepoTree(repoObj.owner.login, name);
  let parsed = [];

  for (let i = 0; i < files.length; i++) {
    progress.repo_index = repoIndex;
    progress.file_index = i;
    saveProgress();

    const raw = await fetchRaw(repoObj.owner.login, name, files[i]);
    if (!raw || !raw.properties) continue;

    const rec = parseRecord(raw);
    if (rec) parsed.push(rec);

    if (i % 200 === 0) await sleep(200);
  }

  if (!parsed.length) return;

  const roots = buildTree(parsed);

  for (const root of roots) {
    const countryBase = path.join(ROOT, code);
    await ensureDir(countryBase);
    await createTree(root, countryBase);
  }

  progress.file_index = 0;
  saveProgress();
}

// -----------------------------------------------
// MAIN
// -----------------------------------------------
(async function main() {
  console.log("Hybrid V4 started");

  const repos = await listAdminRepos();
  const adminRepos = repos.filter(r => r.name.startsWith("whosonfirst-data-admin-"));

  for (let ri = progress.repo_index; ri < adminRepos.length; ri++) {
    try {
      await processRepo(adminRepos[ri], ri);
    } catch (e) {
      console.log("Repo failed", adminRepos[ri].name, e.message);
      await sleep(2000);
    }
  }

  console.log("DONE");
})();
