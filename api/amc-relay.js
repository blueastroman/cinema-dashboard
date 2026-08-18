const crypto = require('node:crypto');

const AMC_API_BASE = 'https://api.amctheatres.com';
const ALLOWED_QUERY_KEYS = new Set(['page-size', 'page-number']);
const ALLOWED_PATHS = [
  /^\/v2\/theatres$/,
  /^\/v2\/theatres\/\d+\/showtimes\/\d{2}-\d{2}-\d{4}$/,
];
const AMC_HEADERS = {
  Accept: 'application/json',
  'Accept-Language': 'en-US,en;q=0.9',
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
};

function respondJson(res, status, payload) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Cache-Control', 'private, no-store');
  res.end(JSON.stringify(payload));
}

function authorized(header, expectedToken) {
  const supplied = String(header || '').replace(/^Bearer\s+/i, '');
  const expected = String(expectedToken || '');
  if (!supplied || !expected) return false;
  const left = Buffer.from(supplied);
  const right = Buffer.from(expected);
  return left.length === right.length && crypto.timingSafeEqual(left, right);
}

function singleQueryValue(value) {
  return Array.isArray(value) ? value[0] : value;
}

function normalizedVendorKey(value) {
  const key = String(value || '').trim();
  if (
    key.length >= 2
    && ((key.startsWith('"') && key.endsWith('"'))
      || (key.startsWith("'") && key.endsWith("'")))
  ) {
    return key.slice(1, -1).trim();
  }
  return key;
}

function buildAmcUrl(query) {
  const path = String(singleQueryValue(query.path) || '');
  if (!ALLOWED_PATHS.some(pattern => pattern.test(path))) {
    throw new Error('Unsupported AMC path');
  }

  const url = new URL(path, AMC_API_BASE);
  for (const [key, rawValue] of Object.entries(query)) {
    if (key === 'path') continue;
    if (!ALLOWED_QUERY_KEYS.has(key)) {
      throw new Error(`Unsupported query parameter: ${key}`);
    }
    const value = String(singleQueryValue(rawValue) || '');
    if (!/^\d{1,3}$/.test(value)) {
      throw new Error(`Invalid ${key}`);
    }
    url.searchParams.set(key, value);
  }
  return url;
}

module.exports = async (req, res) => {
  if (req.method !== 'GET') {
    res.setHeader('Allow', 'GET');
    return respondJson(res, 405, { error: 'Method not allowed' });
  }
  if (!process.env.AMC_RELAY_TOKEN || !process.env.AMC_VENDOR_KEY) {
    return respondJson(res, 503, { error: 'AMC relay is not configured' });
  }
  if (!authorized(req.headers.authorization, process.env.AMC_RELAY_TOKEN)) {
    return respondJson(res, 401, { error: 'Unauthorized' });
  }

  let url;
  try {
    url = buildAmcUrl(req.query || {});
  } catch (error) {
    return respondJson(res, 400, {
      error: error instanceof Error ? error.message : 'Invalid request',
    });
  }

  try {
    const vendorKey = normalizedVendorKey(process.env.AMC_VENDOR_KEY);
    const response = await fetch(url, {
      headers: {
        ...AMC_HEADERS,
        'X-AMC-Vendor-Key': vendorKey,
      },
      signal: AbortSignal.timeout(20_000),
    });
    const body = await response.text();
    if (!response.ok) {
      console.error(JSON.stringify({
        event: 'amc_upstream_error',
        status: response.status,
        path: url.pathname,
        detail: body.replace(/\s+/g, ' ').trim().slice(0, 500),
      }));
    }
    res.statusCode = response.status;
    res.setHeader(
      'Content-Type',
      response.headers.get('content-type') || 'application/json; charset=utf-8',
    );
    res.setHeader('Cache-Control', 'private, no-store');
    return res.end(body);
  } catch (error) {
    return respondJson(res, 502, {
      error: error instanceof Error ? error.message : 'AMC request failed',
    });
  }
};
