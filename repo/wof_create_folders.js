// =======================================================
// wof_create_folders.v2.js  — HYBRID V2 (PRODUCTION SAFE)
// =======================================================
// Node 18+ | GitHub Actions
// No Overpass | Pure WOF | Cycle-proof
// =======================================================

import { Octokit } from "@octokit/rest";
import axios from "axios";

// ---------------- ENV ----------------
const TOKEN  = process.env.PAT_TOKEN;
const OWNER  = process.env.GITHUB_OWNER || process.env.GITHUB_REPOSITORY.split("/")[0];
const REPO   = process.env.GITHUB_REPO  || process.env.GITHUB_REPOSITORY.split("/")[1];
const BRANCH = process.env.BRANCH || "main";
const ROOT   = "real-estate";
const WOF_ORG = "whosonfirst-data";

if (!TOKEN) throw new Error("PAT_TOKEN missing");

// ---------------- GITHUB ----------------
const octokit = new Octokit({ auth: TOKEN });

// ---------------- RULES ----------------
const PLACETYPE_CHAIN = {
  country: ["region"],
  region: ["locality"],
  locality: ["neighbourhood"],
  neighbourhood: ["microhood"]
};

const ALLOWED = new Set(Object.keys(PLACETYPE_CHAIN));

// ---------------- HELPERS ----------------
const sleep = ms => new Promise(r => setTimeout(r, ms));

function sanitize(name) {
  return name.normalize("NFKD")
    .replace(/[^\w\s-]/g, "")
    .replace(/\s+/g, "_")
    .trim();
}

async function putFile(path, content) {
  let sha;
  try {
    const r = await octokit.repos.getContent({ owner: OWNER, repo: REPO, path, ref: BRANCH });
    sha = r.data.sha;
  } catch {}

  await octokit.repos.createOrUpdateFileContents({
    owner: OWNER,
    repo: REPO,
    branch: BRANCH,
    path,
    message: `init ${path}`,
    content: Buffer.from(content).toString("base64"),
    sha
  });
}

async function ensureFolder(path) {
  await putFile(`${path}/.keep`, JSON.stringify({ created: new Date().toISOString() }, null, 2));
}

async function ensureLeaf(path) {
  await putFile(`${path}/buyers.json`, JSON.stringify({ buyers: [] }, null, 2));
  await putFile(`${path}/properties.json`, JSON.stringify({ properties: [] }, null, 2));
  await putFile(`${path}/metadata.json`, JSON.stringify({ created: new Date().toISOString() }, null, 2));
}

// ---------------- WOF ----------------
async function listAdminRepos() {
  return octokit.paginate(octokit.repos.listForOrg, {
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
    .filter(t => t.path.endsWith(".geojson"))
    .map(t => t.path);
}

async function fetchRaw(owner, repo, path) {
  const url = `https://raw.githubusercontent.com/${owner}/${repo}/main/${path}`;
  try {
    return (await axios.get(url, { timeout: 60000 })).data;
  } catch {
    return null;
  }
}

// ---------------- PARSE ----------------
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

// ---------------- GRAPH ----------------
function buildGraph(records) {
  const map = new Map();
  records.forEach(r => map.set(r.id, r));

  for (const r of records) {
    const parent = map.get(r.parent);
    if (!parent) continue;

    const allowedChildren = PLACETYPE_CHAIN[parent.placetype];
    if (!allowedChildren) continue;

    if (allowedChildren.includes(r.placetype)) {
      parent.children.push(r);
    }
  }

  return [...map.values()].filter(r => r.placetype === "country");
}

// ---------------- TREE CREATION ----------------
async function createTree(node, basePath, visited = new Set()) {
  if (visited.has(node.id)) return;
  visited.add(node.id);

  const path = `${basePath}/${sanitize(node.name)}`;
  await ensureFolder(path);

  if (!node.children.length) {
    await ensureLeaf(path);
    return;
  }

  for (const ch of node.children) {
    await createTree(ch, path, visited);
  }
}

// ---------------- MAIN ----------------
(async function main() {
  console.log("HYBRID V2 — WOF hierarchy build started");

  const repos = await listAdminRepos();
  const adminRepos = repos.filter(r => r.name.startsWith("whosonfirst-data-admin-"));

  for (const repo of adminRepos) {
    console.log("Processing:", repo.name);

    const files = await fetchRepoTree(repo.owner.login, repo.name);
    const records = [];

    for (const f of files) {
      const raw = await fetchRaw(repo.owner.login, repo.name, f);
      if (!raw) continue;
      const rec = parseRecord(raw);
      if (rec) records.push(rec);
    }

    const roots = buildGraph(records);

    for (const country of roots) {
      await createTree(country, ROOT);
    }

    await sleep(1000);
  }

  console.log("HYBRID V2 COMPLETE — folders created correctly");
})();
