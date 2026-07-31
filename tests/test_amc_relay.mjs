import assert from "node:assert/strict";
import test from "node:test";

import relay from "../public/api/amc-relay.js";

function responseRecorder() {
  return {
    headers: {},
    statusCode: 200,
    setHeader(name, value) {
      this.headers[name.toLowerCase()] = value;
    },
    end(body = "") {
      this.body = body;
    },
  };
}

async function withRelayEnv(callback) {
  const previousToken = process.env.AMC_RELAY_TOKEN;
  const previousVendorKey = process.env.AMC_VENDOR_KEY;
  process.env.AMC_RELAY_TOKEN = "relay-secret";
  process.env.AMC_VENDOR_KEY = "vendor-secret";
  try {
    await callback();
  } finally {
    if (previousToken === undefined) delete process.env.AMC_RELAY_TOKEN;
    else process.env.AMC_RELAY_TOKEN = previousToken;
    if (previousVendorKey === undefined) delete process.env.AMC_VENDOR_KEY;
    else process.env.AMC_VENDOR_KEY = previousVendorKey;
  }
}

test("relay rejects requests without its bearer token", async () => {
  await withRelayEnv(async () => {
    const res = responseRecorder();
    await relay(
      { method: "GET", headers: {}, query: { path: "/v2/theatres" } },
      res,
    );
    assert.equal(res.statusCode, 401);
  });
});

test("relay rejects arbitrary upstream paths", async () => {
  await withRelayEnv(async () => {
    const res = responseRecorder();
    await relay(
      {
        method: "GET",
        headers: { authorization: "Bearer relay-secret" },
        query: { path: "https://example.com/private" },
      },
      res,
    );
    assert.equal(res.statusCode, 400);
    assert.match(res.body, /Unsupported AMC path/);
  });
});

test("relay forwards an allowed AMC request with the vendor key", async () => {
  await withRelayEnv(async () => {
    const requests = [];
    const originalFetch = globalThis.fetch;
    globalThis.fetch = async (url, options) => {
      requests.push([url, options]);
      return Response.json({ count: 0, _embedded: { theatres: [] } });
    };
    try {
      const res = responseRecorder();
      await relay(
        {
          method: "GET",
          headers: { authorization: "Bearer relay-secret" },
          query: {
            path: "/v2/theatres",
            "page-size": "100",
            "page-number": "1",
          },
        },
        res,
      );
      assert.equal(res.statusCode, 200);
      assert.equal(requests.length, 1);
      assert.equal(
        String(requests[0][0]),
        "https://api.amctheatres.com/v2/theatres?page-size=100&page-number=1",
      );
      assert.equal(
        requests[0][1].headers["X-AMC-Vendor-Key"],
        "vendor-secret",
      );
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});

test("relay removes accidental whitespace and quotes from the vendor key", async () => {
  await withRelayEnv(async () => {
    process.env.AMC_VENDOR_KEY = '  "vendor-secret"  ';
    const originalFetch = globalThis.fetch;
    globalThis.fetch = async (_url, options) => {
      assert.equal(options.headers["X-AMC-Vendor-Key"], "vendor-secret");
      return Response.json({ count: 0, _embedded: { theatres: [] } });
    };
    try {
      const res = responseRecorder();
      await relay(
        {
          method: "GET",
          headers: { authorization: "Bearer relay-secret" },
          query: { path: "/v2/theatres" },
        },
        res,
      );
      assert.equal(res.statusCode, 200);
    } finally {
      globalThis.fetch = originalFetch;
    }
  });
});
