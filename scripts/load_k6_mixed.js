import http from "k6/http";
import { check, sleep } from "k6";

const baseUrl = __ENV.ELO_BASE_URL || "http://127.0.0.1:3000";
const apiKey = __ENV.ELO_API_KEY || "seu_token";
const users = parseInt(__ENV.SEED_USERS || "100000", 10);
const teams = parseInt(__ENV.SEED_TEAMS || "10000", 10);
const teamType = __ENV.REC_TYPE || "team";
const numKey = __ENV.REC_NUM_KEY || "rating";
const min = __ENV.REC_MIN || "300";
const max = __ENV.REC_MAX || "900";
const limit = __ENV.REC_LIMIT || "10";

export const options = {
  vus: parseInt(__ENV.K6_VUS || "20", 10),
  duration: __ENV.K6_DURATION || "30s",
};

function getNode(userId) {
  const url = `${baseUrl}/nodes/${encodeURIComponent(`user:${userId}`)}`;
  const res = http.get(url, { headers: { "x-api-key": apiKey } });
  check(res, { "nodes status 200": (r) => r.status === 200 });
}

function listNodes(teamType) {
  const url = `${baseUrl}/nodes?type=${encodeURIComponent(teamType)}`;
  const res = http.get(url, { headers: { "x-api-key": apiKey } });
  check(res, { "list nodes 200": (r) => r.status === 200 });
}

function listEdges() {
  const url = `${baseUrl}/edges?type=owner`;
  const res = http.get(url, { headers: { "x-api-key": apiKey } });
  check(res, { "list edges 200": (r) => r.status === 200 });
}

function recommendations(userId) {
  const url = `${baseUrl}/recommendations?start=${encodeURIComponent(
    `user:${userId}`
  )}&type=${encodeURIComponent(teamType)}&num_key=${encodeURIComponent(
    numKey
  )}&min=${encodeURIComponent(min)}&max=${encodeURIComponent(
    max
  )}&limit=${encodeURIComponent(limit)}`;
  const res = http.get(url, { headers: { "x-api-key": apiKey } });
  check(res, { "recommendations 200": (r) => r.status === 200 });
}

export default function () {
  const roll = Math.random();
  const userId = Math.floor(Math.random() * users);
  if (roll < 0.6) {
    recommendations(userId);
  } else if (roll < 0.8) {
    getNode(userId);
  } else if (roll < 0.9) {
    listNodes("team");
  } else {
    listEdges();
  }
  sleep(0.1);
}
