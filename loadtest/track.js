// k6 ingress load test. Ramps virtual users posting track events.
//
//   API_URL=http://localhost:18080 API_KEY=cdp_... \
//   docker run --rm -i --network host \
//     -e API_URL -e API_KEY grafana/k6 run - < loadtest/track.js
import http from "k6/http";
import { check } from "k6";

const API_URL = __ENV.API_URL || "http://localhost:18080";
const API_KEY = __ENV.API_KEY;

export const options = {
  stages: [
    { duration: "10s", target: 20 },
    { duration: "30s", target: 50 },
    { duration: "10s", target: 0 },
  ],
  thresholds: {
    http_req_duration: ["p(95)<250"],
    http_req_failed: ["rate<0.01"],
  },
};

export default function () {
  const payload = JSON.stringify({
    user_id: `u-${__VU}-${__ITER}`,
    event_name: "product_viewed",
    properties: { product_id: "p001", category: "phone" },
  });
  const res = http.post(`${API_URL}/v1/events/track`, payload, {
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${API_KEY}` },
  });
  check(res, { "status is 202": (r) => r.status === 202 });
}
