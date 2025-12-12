// wof_create_folders.js
// Usage: NODE_ENV=production node wof_create_folders.js
// Requires: npm i dotenv @octokit/rest p-queue fast-glob
// Expects local WOF GeoJSON/JSON files under ./data/wof/** (recursive)
//
// Behavior:
//  - Scans data/wof for .geojson/.json files, extracts properties:
//      id (wof:id | properties['wof:id'] | properties.wof_id ...)
//      parent_id (wof:parent_id | properties['wof:parent_id'] | parent_id ...)
//      placetype
//      name (properties.name || properties['name:en'] fallback)
//  - Builds id->node map and children relations
//  - Finds roots where placetype === 'country' (if none, tries top-level with no parent)
//  - For each country, creates folders in GitHub: country/.keep then recurses states->cities->localities->sublocalities
//  - Creates buyers.json, properties.json, metadata.json at leaves
//  - Uses Octokit (GITHUB_TOKEN) to create/update files (branch from .env BRANCH or main)
//
// NOTE: large datasets should be added to the repo via Git LFS (see instructions below)

import dotenv from "dotenv";
dotenv.config();

import fs from "fs/promises";
import path from "path";
import fg from "fast-glob";
import PQueue from "p-queue";
import { Octokit } from "@octokit/rest";

const DATA_DIR = process.env.WOF_DATA_DIR || "data/wof";
const OWNER = process.env.GITHUB_OWNER;
const REPO = process.env.GITHUB_REPO;
const BRANCH = process.env.BRANCH || "main";
const TOKEN = process.env.GITHUB_TOKEN;
const CONCURRENCY = parseInt(process.env.WOF_CONCURRENCY || "4", 10);

if (!OWNER || !REPO || !TOKEN) {
  console.error("Missing GITHUB_OWNER / GITHUB_REPO / GITHUB_TOKEN in env");
  process.exit(1);
}

const octokit = new Octokit({ auth: TOKEN });
const ghQueue = new PQueue({ concurrency: 1, intervalCap: 1 }); // keep GitHub serial to avoid conflicts

function sanitizeSegment(name) {
  if (!name) return "unnamed";
  return name.toString().normalize("NFKD")
    .replace(/[\/:*?"<>|\\'#%]/g, "_")
    .replace(/\s+/g, "_")
    .replace(/_+/g, "_")
    .trim();
}

function pickBestName(props) {
  if (!props) return null;
  return props.name || props["name:en"] || props["wof:name"] || props["geom_name"] || props["label"] || null;
}

function getPropVariants(props, ...keys) {
  for (const k of keys) {
    if (props?.[k] !== undefined) return props[k];
  }
  return undefined;
}

async function readAllWofFiles() {
  const patterns = [
    `${DATA_DIR}/**/*.geojson`,
    `${DATA_DIR}/**/*.json`
  ];
  const files = await fg(patterns, { dot: false, onlyFiles: true });
  const nodes = [];
  for (const f of files) {
    try {
      const txt = await fs.readFile(f, "utf8");
      const js = JSON.parse(txt);
      // WOF files may be Feature or FeatureCollection; handle both
      if (js.type === "FeatureCollection" && Array.isArray(js.features)) {
        for (const feat of js.features) {
          const props = feat.properties || {};
          const id = getPropVariants(props, "wof:id", "wof_id", "id", "id:wof");
          const parent = getPropVariants(props, "wof:parent_id", "wof_parent", "parent_id", "parent");
          const placetype = getPropVariants(props, "placetype", "wof:placetype");
          const name = pickBestName(props);
          if (id && name) nodes.push({ id: Number(id), parent: parent ? Number(parent) : null, placetype, name, rawProps: props });
        }
      } else if (js.type === "Feature") {
        const props = js.properties || {};
        const id = getPropVariants(props, "wof:id", "wof_id", "id");
        const parent = getPropVariants(props, "wof:parent_id", "parent_id", "parent");
        const placetype = getPropVariants(props, "placetype", "wof:placetype");
        const name = pickBestName(props);
        if (id && name) nodes.push({ id: Number(id), parent: parent ? Number(parent) : null, placetype, name, rawProps: props });
      } else if (js.properties) {
        const props = js.properties || {};
        const id = getPropVariants(props, "wof:id", "wof_id", "id");
        const parent = getPropVariants(props, "wof:parent_id", "parent_id", "parent");
        const placetype = getPropVariants(props, "placetype", "wof:placetype");
        const name = pickBestName(props);
        if (id && name) nodes.push({ id: Number(id), parent: parent ? Number(parent) : null, placetype, name, rawProps: props });
      } else {
        // try top-level fields
        const props = js;
        const id = getPropVariants(props, "wof:id", "wof_id", "id");
        const parent = getPropVariants(props, "wof:parent_id", "parent_id", "parent");
        const placetype = getPropVariants(props, "placetype", "wof:placetype");
        const name = pickBestName(props);
        if (id && name) nodes.push({ id: Number(id), parent: parent ? Number(parent) : null, placetype, name, rawProps: props });
      }
    } catch (e) {
      // ignore parse errors — files may be non-WOF
    }
  }
  return nodes;
}

function buildTree(nodes) {
  const map = new Map();
  for (const n of nodes) map.set(n.id, { ...n, children: [] });
  for (const [id, n] of map.entries()) {
    if (n.parent && map.has(n.parent)) {
      map.get(n.parent).children.push(n);
    }
  }
  return map;
}

async function githubGetSha(path) {
  try {
    const res = await octokit.repos.getContent({ owner: OWNER, repo: REPO, path, ref: BRANCH });
    if (res && res.data && res.data.sha) return res.data.sha;
  } catch (e) {
    if (e.status !== 404) {
      console.warn("GitHub getContent error", e.status || e.message);
    }
  }
  return null;
}

async function githubPut(path, content, message) {
  return ghQueue.add(async () => {
    const encoded = Buffer.from(content, "utf8").toString("base64");
    const sha = await githubGetSha(path);
    try {
      await octokit.repos.createOrUpdateFileContents({
        owner: OWNER, repo: REPO, path, message,
        content: encoded, branch: BRANCH, sha: sha || undefined
      });
      return true;
    } catch (e) {
      console.warn("GitHub put failed", path, e.status || e.message);
      return false;
    }
  });
}

async function createFolderPlaceholder(folderPath) {
  const clean = folderPath.replace(/^\/+|\/+$/g, "");
  const p = `${clean}/.keep`;
  const body = JSON.stringify({ created: new Date().toISOString(), source: "wof-create" }, null, 2);
  return githubPut(p, body, `Create placeholder ${clean}`);
}

async function createLeafFiles(folderPath) {
  const clean = folderPath.replace(/^\/+|\/+$/g, "");
  await githubPut(`${clean}/buyers.json`, JSON.stringify({ buyers: [] }, null, 2), `Init buyers for ${clean}`);
  await githubPut(`${clean}/properties.json`, JSON.stringify({ properties: [] }, null, 2), `Init properties for ${clean}`);
  await githubPut(`${clean}/metadata.json`, JSON.stringify({ created: new Date().toISOString() }, null, 2), `Init metadata for ${clean}`);
}

function nodeToSegment(node) {
  const seg = sanitizeSegment(node.name);
  // add wof id to avoid collisions (optional)
  return `${seg}`;
}

async function recurseCreate(node, parentPath, map) {
  // node: contains id,name,placetype,children[]
  const seg = nodeToSegment(node);
  const myPath = parentPath ? `${parentPath}/${seg}` : seg;

  // create placeholder for this node
  await createFolderPlaceholder(myPath);

  if (!node.children || node.children.length === 0) {
    // leaf
    await createLeafFiles(myPath);
    return;
  }

  // create children concurrently but rate-limited via queue when calling GitHub
  const q = new PQueue({ concurrency: CONCURRENCY });
  for (const ch of node.children) {
    q.add(async () => {
      // upsert DB is optional; skip DB operations unless you want them
      await recurseCreate(ch, myPath, map);
    });
  }
  await q.onIdle();
}

(async function main() {
  console.log("WOF folder builder starting — scanning local WOF files...");
  const nodesArr = await readAllWofFiles();
  if (!nodesArr || nodesArr.length === 0) {
    console.error("No WOF nodes found under", DATA_DIR);
    process.exit(1);
  }

  const map = buildTree(nodesArr);
  // find country roots
  const countries = [];
  for (const node of map.values()) {
    if (node.placetype === "country") countries.push(node);
  }
  // fallback: nodes without parent
  if (countries.length === 0) {
    for (const node of map.values()) {
      if (!node.parent) countries.push(node);
    }
  }

  console.log("Found countries:", countries.length);
  for (const c of countries) {
    // create root placeholder
    const rootSeg = nodeToSegment(c);
    const rootPath = `${rootSeg}`;
    console.log("Processing country:", c.name, "->", rootPath);
    await createFolderPlaceholder(rootPath);
    if (!c.children || c.children.length === 0) {
      await createLeafFiles(rootPath);
      continue;
    }
    // sequentially recurse for each child to avoid concurrent writes that create conflicts
    for (const st of c.children) {
      await recurseCreate(st, rootPath, map);
    }
    // after finishing children, mark root leaf metadata
    await githubPut(`${rootPath}/metadata.json`, JSON.stringify({ created: new Date().toISOString() }, null, 2), `country metadata ${rootPath}`);
  }

  console.log("Done. All country folders attempted.");
})();
