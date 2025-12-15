/* =====================================================================
   HYBRID V3 — WOF Hierarchy Generator
   GitHub Batch Commit Edition (S1 = 500 folders/commit)
   =====================================================================

   FEATURES:
   - No Overpass, 100% WOF
   - Fetches all WOF admin repos (country/state/city/locality/microhood)
   - Builds full hierarchy graph safely
   - Batches folder placeholders into commits of 500 folders
   - Creates leaf structure (buyers.json / properties.json / metadata.json)
   - Uses Git Trees API → FAST + avoids 409
   - Resume support via .wof_checkpoint.json
   - Error-safe, cycle-safe

   REQUIREMENTS:
   Node 18+
   npm i axios @octokit/rest

   ENV REQUIRED:
   PAT_TOKEN     → GitHub PAT (repo scope)
   GITHUB_OWNER  → repo owner
   GITHUB_REPO   → repo name
   BRANCH        → branch to write to (main)
   ===================================================================== */

import { Octokit } from "@octokit/rest";
import axios from "axios";
import fs from "fs/promises";

// ---------------------------------------------------------------------
// CONFIG
// ---------------------------------------------------------------------
const TOKEN  = process.env.PAT_TOKEN;
const OWNER  = process.env.GITHUB_OWNER || process.env.GITHUB_REPOSITORY.split("/")[0];
const REPO   = process.env.GITHUB_REPO  || process.env.GITHUB_REPOSITORY.split("/")[1];
const BRANCH = process.env.BRANCH || "main";

const ROOT = "real-estate";
const WOF_ORG = "whosonfirst-data";

const BATCH_LIMIT = 500;   // S1 MODE — 500 folder nodes per commit

if (!TOKEN) throw new Error("PAT_TOKEN missing");

const octokit = new Octokit({ auth: TOKEN });

const sleep = ms => new Promise(r => setTimeout(r, ms));

function sanitize(name) {
  return (name || "unnamed")
    .normalize("NFKD")
    .replace(/[^\w\s-]/g, "")
    .replace(/\s+/g, "_")
    .trim();
}

// ---------------------------------------------------------------------
// PLACETYPE RULES
// ---------------------------------------------------------------------
const PLACETYPE_CHAIN = {
  country: ["region"],
  region: ["locality"],
  locality: ["neighbourhood"],
  neighbourhood: ["microhood"]
};

const ALLOWED = new Set(Object.keys(PLACETYPE_CHAIN));

// ---------------------------------------------------------------------
// CHECKPOINT
// ---------------------------------------------------------------------
const CHECKPOINT_FILE = ".wof_checkpoint.json";

async function loadCheckpoint() {
  try {
    const raw = await fs.readFile(CHECKPOINT_FILE, "utf8");
    return JSON.parse(raw);
  } catch {
    return { lastRepo: null };
  }
}

async function saveCheckpoint(obj) {
  await fs.writeFile(CHECKPOINT_FILE, JSON.stringify(obj, null, 2));
}

// ---------------------------------------------------------------------
// GITHUB TREE CREATION
// ---------------------------------------------------------------------
async function getLatestCommitSha(branch) {
  const ref = await octokit.git.getRef({ owner: OWNER, repo: REPO, ref: `heads/${branch}` });
  return ref.data.object.sha;
}

async function getTreeSha(commitSha) {
  const commit = await octokit.git.getCommit({ owner: OWNER, repo: REPO, commit_sha: commitSha });
  return commit.data.tree.sha;
}

async function createTree(treeItems) {
  const res = await octokit.git.createTree({
    owner: OWNER,
    repo: REPO,
    tree: treeItems
  });
  return res.data.sha;
}

async function createCommit(message, treeSha, parentSha) {
  const res = await octokit.git.createCommit({
    owner: OWNER,
    repo: REPO,
    message,
    tree: treeSha,
    parents: [parentSha]
  });
  return res.data.sha;
}

async function updateBranch(commitSha) {
  await octokit.git.updateRef({
    owner: OWNER,
    repo: REPO,
    ref: `heads/${BRANCH}`,
    sha: commitSha,
    force: true
  });
}

// ---------------------------------------------------------------------
// BATCH ENGINE
// ---------------------------------------------------------------------
let pendingTree = [];
let batchCounter = 0;

async function flushBatch() {
  if (pendingTree.length === 0) return;

  const parentCommit = await getLatestCommitSha(BRANCH);

  const treeSha = await createTree(pendingTree);

  const commitSha = await createCommit(
    `batch commit (${pendingTree.length} items)`,
    treeSha,
    parentCommit
  );

  await updateBranch(commitSha);

  console.log(`✔ committed batch of ${pendingTree.length}`);

  pendingTree = [];
}

async function addFolderNode(path) {
  const item = {
    path: `${path}/.keep`,
    mode: "100644",
    type: "blob",
    content: JSON.stringify({ created: new Date().toISOString() })
  };
  pendingTree.push(item);

  if (pendingTree.length >= BATCH_LIMIT) {
    await flushBatch();
  }
}

async function addLeafFiles(path) {
  const buyers = { buyers: [] };
  const props  = { properties: [] };
  const meta   = { created: new Date().toISOString() };

  pendingTree.push({
    path: `${path}/buyers.json`,
    mode: "100644",
    type: "blob",
    content: JSON.stringify(buyers, null, 2)
  });
  pendingTree.push({
    path: `${path}/properties.json`,
    mode: "100644",
    type: "blob",
    content: JSON.stringify(props, null, 2)
  });
  pendingTree.push({
    path: `${path}/metadata.json`,
    mode: "100644",
    type: "blob",
    content: JSON.stringify(meta, null, 2)
  });

  if (pendingTree.length >= BATCH_LIMIT) {
    await flushBatch();
  }
}

// ---------------------------------------------------------------------
// WOF FETCHING
// ---------------------------------------------------------------------
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

// ---------------------------------------------------------------------
// PARSE RECORD
// ---------------------------------------------------------------------
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

// ---------------------------------------------------------------------
// BUILD GRAPH
// ---------------------------------------------------------------------
function buildGraph(records) {
  const map = new Map();
  records.forEach(r => map.set(r.id, r));

  for (const rec of records) {
    const parent = map.get(rec.parent);
    if (!parent) continue;

    const allowedKids = PLACETYPE_CHAIN[parent.placetype];
    if (!allowedKids) continue;

    if (allowedKids.includes(rec.placetype)) {
      parent.children.push(rec);
    }
  }

  return [...map.values()].filter(r => r.placetype === "country");
}

// ---------------------------------------------------------------------
// CREATE TREE RECURSIVELY
// ---------------------------------------------------------------------
async function createTree(node, basePath, visited = new Set()) {
  if (visited.has(node.id)) return;
  visited.add(node.id);

  const path = `${basePath}/${sanitize(node.name)}`;

  await addFolderNode(path);

  if (node.children.length === 0) {
    await addLeafFiles(path);
    return;
  }

  for (const ch of node.children) {
    await createTree(ch, path, visited);
  }
}

// ---------------------------------------------------------------------
// MAIN
// ---------------------------------------------------------------------
(async function main() {
  console.log("HYBRID V3 STARTED — batching enabled (500 folders/commit)");

  const checkpoint = await loadCheckpoint();
  const repos = await listAdminRepos();

  const adminRepos = repos.filter(r => r.name.startsWith("whosonfirst-data-admin-"));

  let skip = checkpoint.lastRepo != null;

  for (const repo of adminRepos) {
    if (skip) {
      if (repo.name === checkpoint.lastRepo) skip = false;
      continue;
    }

    console.log("Processing repo:", repo.name);

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

    await saveCheckpoint({ lastRepo: repo.name });

    await sleep(1000);
  }

  // flush remaining
  await flushBatch();

  console.log("HYBRID V3 COMPLETED — all folders + leaf files created.");
})();
