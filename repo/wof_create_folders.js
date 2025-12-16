// ===========================================================
// HYBRID V3 — LOCAL FILESYSTEM VERSION (NO API RATE LIMIT)
// ===========================================================
// Creates full WOF hierarchy on runner filesystem,
// then commits all changes in one push.
// ===========================================================

import fs from "fs";
import fsp from "fs/promises";
import path from "path";
import axios from "axios";
import { Octokit } from "@octokit/rest";

// ---------------- ENV ----------------
const TOKEN  = process.env.PAT_TOKEN;
const GITHUB_REPOSITORY = process.env.GITHUB_REPOSITORY;
const OWNER  = GITHUB_REPOSITORY.split("/")[0];
const REPO   = GITHUB_REPOSITORY.split("/")[1];

const ROOT_DIR = "real-estate";
const WOF_ORG  = "whosonfirst-data";

// ---------------- GitHub ----------------
const octokit = new Octokit({ auth: TOKEN });

// ---------------- Helpers ----------------
const sleep = ms => new Promise(r => setTimeout(r, ms));

function sanitize(name) {
  return name
    .normalize("NFKD")
    .replace(/[^\w\s-]/g, "")
    .replace(/\s+/g, "_")
    .trim();
}

async function ensureDir(dir) {
  await fsp.mkdir(dir, { recursive: true });
}

async function writeJSON(filePath, obj) {
  await fsp.writeFile(filePath, JSON.stringify(obj, null, 2));
}

async function ensureLeaf(dir) {
  await writeJSON(path.join(dir, "buyers.json"),     { buyers: [] });
  await writeJSON(path.join(dir, "properties.json"), { properties: [] });
  await writeJSON(path.join(dir, "metadata.json"),   { created: new Date().toISOString() });
}

// ---------------- WOF ----------------
async function listAdminRepos() {
  return await octokit.paginate(octokit.repos.listForOrg, {
    org: WOF_ORG,
    per_page: 100
  });
}

async function fetchRepoTree(owner, repo) {
  const meta = await octokit.repos.get({ owner, repo });
  const tree = await octokit.git.getTree({
    owner,
    repo,
    tree_sha: meta.data.default_branch,
    recursive: "1"
  });
  return tree.data.tree
    .map(t => t.path)
    .filter(p => p.endsWith(".geojson"));
}

async function fetchRaw(owner, repo, pathInRepo) {
  const url = `https://raw.githubusercontent.com/${owner}/${repo}/main/${pathInRepo}`;
  try {
    return (await axios.get(url, { timeout: 60000 })).data;
  } catch {
    return null;
  }
}

// ---------------- Parsing ----------------
const ALLOWED = new Set(["country", "region", "locality", "neighbourhood", "microhood"]);

const CHILD_MAP = {
  country: ["region"],
  region: ["locality"],
  locality: ["neighbourhood"],
  neighbourhood: ["microhood"]
};

function parseRecord(obj) {
  const p = obj?.properties;
  if (!p) return null;

  const placetype = p["wof:placetype"];
  if (!ALLOWED.has(placetype)) return null;

  return {
    id: String(p["wof:id"]),
    name: p["wof:name"],
    placetype,
    parent: String(p["wof:parent_id"] || ""),
    children: []
  };
}

function buildGraph(records) {
  const map = new Map();
  records.forEach(r => map.set(r.id, r));

  for (const r of records) {
    const parent = map.get(r.parent);
    if (!parent) continue;

    const allowed = CHILD_MAP[parent.placetype] || [];
    if (allowed.includes(r.placetype)) {
      parent.children.push(r);
    }
  }

  return [...map.values()].filter(r => r.placetype === "country");
}

// ---------------- Tree Build (LOCAL) ----------------
async function createTree(node, baseDir, visited = new Set()) {
  if (visited.has(node.id)) return;
  visited.add(node.id);

  const thisDir = path.join(baseDir, sanitize(node.name));
  await ensureDir(thisDir);

  if (!node.children.length) {
    await ensureLeaf(thisDir);
    return;
  }

  for (const ch of node.children) {
    await createTree(ch, thisDir, visited);
  }
}

// ---------------- MAIN ----------------
(async function main() {
  console.log("Hybrid V3 started");

  await ensureDir(ROOT_DIR);

  const repos = await listAdminRepos();
  const adminRepos = repos.filter(r => r.name.startsWith("whosonfirst-data-admin-"));

  for (const repo of adminRepos) {
    console.log("Processing repo:", repo.name);

    const files = await fetchRepoTree(repo.owner.login, repo.name);
    const recs = [];

    for (const f of files) {
      const raw = await fetchRaw(repo.owner.login, repo.name, f);
      if (!raw) continue;
      const rec = parseRecord(raw);
      if (rec) recs.push(rec);
    }

    const roots = buildGraph(recs);

    for (const country of roots) {
      await createTree(country, ROOT_DIR);
    }

    await sleep(500);
  }

  console.log("Local tree build complete. Git commit happens in workflow.");
})();
