// repo/wof_create_folders.js
// Node 18+
// Installs: axios @octokit/rest p-queue (workflow installs them on the runner)

import { Octokit } from "@octokit/rest";
import axios from "axios";
import PQueue from "p-queue";
import fs from "fs/promises";
import path from "path";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const PAT_TOKEN = process.env.PAT_TOKEN;
const OWNER = process.env.GITHUB_OWNER || process.env.GITHUB_REPOSITORY?.split("/")[0];
const REPO = process.env.GITHUB_REPO || process.env.GITHUB_REPOSITORY?.split("/")[1];
const BRANCH = process.env.BRANCH || "main";
const TARGET_DIR = process.env.TARGET_DIR || "data/wof";
const WOF_ORG = process.env.WOF_ORG || "whosonfirst-data";

const COUNTRY_CONCURRENCY = parseInt(process.env.COUNTRY_CONCURRENCY || "1", 10);
const CITY_CONCURRENCY = parseInt(process.env.CITY_CONCURRENCY || "4", 10);
const LOCALITY_CONCURRENCY = parseInt(process.env.LOCALITY_CONCURRENCY || "4", 10);

if (!PAT_TOKEN) {
  console.error("PAT_TOKEN required (store as repo secret).");
  process.exit(1);
}
if (!OWNER || !REPO) {
  console.error("GITHUB_OWNER and GITHUB_REPO required.");
  process.exit(1);
}

const octokit = new Octokit({ auth: PAT_TOKEN });

// helper sanitize
function sanitize(s) {
  if (!s) return "unnamed";
  return s.toString()
    .normalize("NFKD")
    .replace(/[\/:*?"<>|\\'#%]/g, "_")
    .replace(/\s+/g, "_")
    .replace(/_+/g, "_")
    .trim();
}

// create or update file (base64 content) using REST API
async function githubPutFile(pathInRepo, contentStr, msg) {
  try {
    // get current sha if exists
    let sha = null;
    try {
      const res = await octokit.repos.getContent({
        owner: OWNER, repo: REPO, path: pathInRepo, ref: BRANCH
      });
      if (res && res.data && res.data.sha) sha = res.data.sha;
    } catch (err) {
      if (err.status !== 404) console.warn("getContent error", err.status);
    }

    await octokit.repos.createOrUpdateFileContents({
      owner: OWNER,
      repo: REPO,
      path: pathInRepo,
      message: msg,
      content: Buffer.from(contentStr, "utf8").toString("base64"),
      branch: BRANCH,
      sha: sha || undefined
    });
    return true;
  } catch (err) {
    console.error("githubPutFile failed", pathInRepo, err.status || err.message || err);
    return false;
  }
}

async function createFolderPlaceholder(folderPath) {
  const clean = folderPath.replace(/^\/+|\/+$/g, "");
  const keepPath = `${clean}/.keep`;
  const content = JSON.stringify({ created: new Date().toISOString(), note: "placeholder" }, null, 2);
  return githubPutFile(keepPath, content, `Init folder ${clean}`);
}

async function createLeafFiles(folderPath) {
  const base = folderPath.replace(/^\/+|\/+$/g, "");
  await githubPutFile(`${base}/buyers.json`, JSON.stringify({ buyers: [] }, null, 2), `init buyers for ${base}`);
  await githubPutFile(`${base}/properties.json`, JSON.stringify({ properties: [] }, null, 2), `init properties for ${base}`);
  await githubPutFile(`${base}/metadata.json`, JSON.stringify({ created: new Date().toISOString() }, null, 2), `init metadata for ${base}`);
}

// list repos for WOF org and filter admin repos (country/state-level repos)
async function listWofAdminRepos() {
  const res = await octokit.paginate(octokit.repos.listForOrg, {
    org: WOF_ORG,
    type: "public",
    per_page: 100
  });
  // filter repos with pattern whosonfirst-data-admin-*
  return res.filter(r => r.name && r.name.startsWith("whosonfirst-data-admin-"));
}

// fetch raw file from a repo (raw github URL)
async function fetchRawFromRepo(owner, repo, filePath) {
  const url = `https://raw.githubusercontent.com/${owner}/${repo}/main/${filePath}`;
  try {
    const r = await axios.get(url, { timeout: 120000 });
    return r.data;
  } catch (err) {
    // file maybe in 'master' branch or path different; fallback: try HEAD branch
    try {
      const repoMeta = await octokit.repos.get({ owner, repo });
      const defaultBranch = repoMeta.data.default_branch || "main";
      const url2 = `https://raw.githubusercontent.com/${owner}/${repo}/${defaultBranch}/${filePath}`;
      const r2 = await axios.get(url2, { timeout: 120000 });
      return r2.data;
    } catch (e) {
      return null;
    }
  }
}

// Find the "canonical" place record files in a repo: we look under ./data/
async function findPlaceFilesForRepo(owner, repo) {
  // try to get a listing under data/ or data/places/... search common paths
  const candidates = [
    "data/places/wof",            // sometimes
    "data/places",                // sometimes
    "data",                       // some repos store top-level
    ""                            // fallback
  ];
  const placeFiles = [];

  // we'll attempt to fetch list of keys under 'data/' using the GitHub tree API
  try {
    // get repo tree for default branch
    const { data: repoMeta } = await octokit.repos.get({ owner, repo });
    const defaultBranch = repoMeta.default_branch || "main";

    const treeRes = await octokit.git.getTree({
      owner, repo, tree_sha: defaultBranch, recursive: "1"
    });
    const allPaths = (treeRes.data.tree || []).map(t => t.path);

    // heuristic: collect .geojson files and .json that look like wof records
    for (const p of allPaths) {
      if (p.endsWith(".geojson") || p.endsWith(".json")) {
        // only include files under a 'data' folder or 'places' folder to avoid large unrelated files
        if (p.includes("/data/") || p.startsWith("data/") || p.includes("/places/") || p.includes("place")) {
          placeFiles.push(p);
        }
      }
    }
  } catch (err) {
    // tree API can fail on some repos or size restrictions; fallback to try a known file
  }

  return placeFiles;
}

// parse a WOF record (geojson) and extract useful bits
function parseWofRecord(obj) {
  if (!obj) return null;
  // WOF records usually have properties: 'properties' contains 'wof:id', 'name', 'wof:parent_id', 'wof:admin_level'
  const props = obj.properties || obj;
  const id = props["wof:id"] || props["id"] || props["wof:placetype"] ? null : null;
  const name = props["wof:name"] || props["properties:fullname"] || props["name"] || (props.names && Object.values(props.names)[0]);
  const parent = props["wof:parent_id"] || props["wof:belongsto"] || props["parent_id"] || null;
  const admin_level = props["wof:admin_level"] || props["admin_level"] || null;
  return {
    id: props["wof:id"] || props["id"] || null,
    wof_id: props["wof:id"] || props["id"] || null,
    name: name || null,
    parent_id: props["wof:parent_id"] || null,
    admin_level
  };
}

// Build hierarchy map in memory from list of records
function buildHierarchy(records) {
  const map = new Map(); // wof_id -> node
  for (const r of records) {
    if (!r || !r.wof_id || !r.name) continue;
    map.set(String(r.wof_id), { ...r, children: [] });
  }
  // attach children
  for (const [k, v] of map) {
    const p = String(v.parent_id || "");
    if (p && map.has(p)) {
      map.get(p).children.push(v);
    }
  }
  return map;
}

// create path segments and push folders/files
async function ensureGitFoldersForNode(node, basePath) {
  const seg = sanitize(node.name);
  const fullPath = `${basePath}/${seg}`;
  await createFolderPlaceholder(fullPath);
  // if leaf (no children) create leaf files
  if (!node.children || node.children.length === 0) {
    await createLeafFiles(fullPath);
  }
  return fullPath;
}

async function processCountryRepo(repoObj) {
  const repoName = repoObj.name;
  console.log("Processing repo:", repoName);

  // find place files
  const placeFiles = await findPlaceFilesForRepo(repoObj.owner.login, repoName);
  if (!placeFiles || placeFiles.length === 0) {
    console.log("No place files detected in repo:", repoName);
    return;
  }

  // fetch the candidate place files, parse JSON, filter only records with admin-level (country/state/city/locality)
  const parsed = [];
  for (const p of placeFiles) {
    try {
      const raw = await fetchRawFromRepo(repoObj.owner.login, repoName, p);
      if (!raw) continue;
      // raw could be JSON string or object already
      const obj = typeof raw === "string" ? JSON.parse(raw) : raw;
      // if this is a FeatureCollection, iterate features
      if (obj.type === "FeatureCollection" && Array.isArray(obj.features)) {
        for (const f of obj.features) {
          const pr = parseWofRecord(f);
          if (pr && pr.wof_id && pr.name) parsed.push(pr);
        }
      } else {
        const pr = parseWofRecord(obj);
        if (pr && pr.wof_id && pr.name) parsed.push(pr);
      }
    } catch (err) {
      // ignore broken files
    }
  }

  if (!parsed.length) {
    console.log("No parseable WOF records in", repoName);
    return;
  }

  // build a hierarchy map for this repo's records
  const map = buildHierarchy(parsed);

  // find top-level nodes (those without a parent in this map)
  const roots = [];
  for (const [k, node] of map) {
    const parent = String(node.parent_id || "");
    if (!parent || !map.has(parent)) roots.push(node);
  }

  // For each root, create folder under TARGET_DIR/<repo-repr> and then recursively create children
  for (const root of roots) {
    const repoBase = `${TARGET_DIR}/${sanitize(repoName)}`;
    await createFolderPlaceholder(repoBase);

    // create root folder
    const rootPath = `${repoBase}/${sanitize(root.name)}`;
    await createFolderPlaceholder(rootPath);

    // BFS/DFS traversal to create folders
    const stack = [{ node: root, path: rootPath }];
    while (stack.length) {
      const { node, path: nodePath } = stack.pop();
      // create placeholder at nodePath (done for root earlier)
      await createFolderPlaceholder(nodePath);

      if (!node.children || node.children.length === 0) {
        await createLeafFiles(nodePath);
      } else {
        // push children
        for (const ch of node.children) {
          const childPath = `${nodePath}/${sanitize(ch.name)}`;
          await createFolderPlaceholder(childPath);
          stack.push({ node: ch, path: childPath });
        }
      }
    }
  }

  console.log("Done repo:", repoName);
}

async function main() {
  console.log("Starting WOF folder generator");
  // list admin repos
  const adminRepos = await listWofAdminRepos();
  if (!adminRepos || adminRepos.length === 0) {
    console.error("No admin repos found under", WOF_ORG);
    return;
  }

  // optionally filter only country-level repos: names like whosonfirst-data-admin-us, -in, etc.
  const countryRepos = adminRepos.filter(r => {
    // many admin repos exist; treat country repos as those ending with two-letter codes
    const m = r.name.match(/^whosonfirst-data-admin-([a-z0-9-]+)$/);
    return !!m;
  });

  console.log("Found admin repos:", countryRepos.length);

  // process sequentially to avoid PUT conflicts
  for (const repoObj of countryRepos) {
    try {
      await processCountryRepo(repoObj);
      // small pause between repos to reduce rate pressure
      await sleep(800);
    } catch (err) {
      console.warn("Repo processing failed for", repoObj.name, (err && err.message) || err);
    }
  }

  console.log("All done");
}

main().catch(err => {
  console.error("Fatal:", err && err.stack ? err.stack : err);
  process.exit(1);
});
