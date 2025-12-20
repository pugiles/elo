import http from "k6/http";
import { check, sleep } from "k6";

const baseUrl = __ENV.ELO_BASE_URL || "http://127.0.0.1:3000";
const apiKey = __ENV.ELO_API_KEY || "seu_token";
const users = parseInt(__ENV.SEED_USERS || "100000", 10);
const teamType = __ENV.REC_TYPE || "team";
const numKey = __ENV.REC_NUM_KEY || "rating";
const min = __ENV.REC_MIN || "300";
const max = __ENV.REC_MAX || "900";
const limit = __ENV.REC_LIMIT || "10";

export const options = {
  vus: parseInt(__ENV.K6_VUS || "20", 10),
  duration: __ENV.K6_DURATION || "30s",
};

export default function () {
  const userId = Math.floor(Math.random() * users);
  const start = `user:${userId}`;
  const url = `${baseUrl}/recommendations?start=${encodeURIComponent(
    start
  )}&type=${encodeURIComponent(teamType)}&num_key=${encodeURIComponent(
    numKey
  )}&min=${encodeURIComponent(min)}&max=${encodeURIComponent(
    max
  )}&limit=${encodeURIComponent(limit)}`;
  const res = http.get(url, {
    headers: { "x-api-key": apiKey },
  });
  check(res, {
    "status is 200": (r) => r.status === 200,
  });
  sleep(0.1);
}
