import http from "k6/http";
import { check, sleep } from "k6";

const baseUrl = __ENV.ELO_BASE_URL || "http://127.0.0.1:3000";
const apiKey = __ENV.ELO_API_KEY || "seu_token";
const users = parseInt(__ENV.SEED_USERS || "100000", 10);
const teams = parseInt(__ENV.SEED_TEAMS || "10000", 10);

const recType = __ENV.REC_TYPE || "team";
const numKey = __ENV.REC_NUM_KEY || "rating";
const min = __ENV.REC_MIN || "300";
const max = __ENV.REC_MAX || "900";
const limit = __ENV.REC_LIMIT || "10";

const recPct = parseFloat(__ENV.REC_PCT || "0.7");
const getPct = parseFloat(__ENV.GET_PCT || "0.15");
const listPct = parseFloat(__ENV.LIST_PCT || "0.1");
const writePct = parseFloat(__ENV.WRITE_PCT || "0.05");

export const options = {
  vus: parseInt(__ENV.K6_VUS || "20", 10),
  duration: __ENV.K6_DURATION || "30s",
};

function recommendations(userId) {
  const url = `${baseUrl}/recommendations?start=${encodeURIComponent(
    `user:${userId}`
  )}&type=${encodeURIComponent(recType)}&num_key=${encodeURIComponent(
    numKey
  )}&min=${encodeURIComponent(min)}&max=${encodeURIComponent(
    max
  )}&limit=${encodeURIComponent(limit)}`;
  const res = http.get(url, { headers: { "x-api-key": apiKey } });
  check(res, { "recommendations 200": (r) => r.status === 200 });
}

function getNode(userId) {
  const url = `${baseUrl}/nodes/${encodeURIComponent(`user:${userId}`)}`;
  const res = http.get(url, { headers: { "x-api-key": apiKey } });
  check(res, { "get node 200": (r) => r.status === 200 });
}

function listNodes() {
  const url = `${baseUrl}/nodes?type=${encodeURIComponent(recType)}`;
  const res = http.get(url, { headers: { "x-api-key": apiKey } });
  check(res, { "list nodes 200": (r) => r.status === 200 });
}

function listEdges() {
  const url = `${baseUrl}/edges?type=owner`;
  const res = http.get(url, { headers: { "x-api-key": apiKey } });
  check(res, { "list edges 200": (r) => r.status === 200 });
}

function writeNodeAndEdge(userId, teamId) {
  const nodeId = `load:${__VU}:${__ITER}`;
  let res = http.post(
    `${baseUrl}/nodes`,
    JSON.stringify({ id: nodeId }),
    {
      headers: { "x-api-key": apiKey, "content-type": "application/json" },
    }
  );
  check(res, { "create node 201": (r) => r.status === 201 });

  res = http.put(
    `${baseUrl}/nodes/${encodeURIComponent(nodeId)}/data`,
    JSON.stringify({ key: "type", value: "load" }),
    {
      headers: { "x-api-key": apiKey, "content-type": "application/json" },
    }
  );
  check(res, { "set node data 204": (r) => r.status === 204 });

  const to = `team:${teamId}`;
  res = http.post(
    `${baseUrl}/edges`,
    JSON.stringify({ from: nodeId, to }),
    {
      headers: { "x-api-key": apiKey, "content-type": "application/json" },
    }
  );
  check(res, { "create edge 201": (r) => r.status === 201 });
}

export default function () {
  const userId = Math.floor(Math.random() * users);
  const teamId = Math.floor(Math.random() * teams);

  const roll = Math.random();
  if (roll < recPct) {
    recommendations(userId);
  } else if (roll < recPct + getPct) {
    getNode(userId);
  } else if (roll < recPct + getPct + listPct) {
    if (Math.random() < 0.5) {
      listNodes();
    } else {
      listEdges();
    }
  } else {
    writeNodeAndEdge(userId, teamId);
  }

  sleep(0.1);
}
