const crypto = require('node:crypto');

function respondJson(res, status, payload) {
  res.statusCode = status;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.end(JSON.stringify(payload));
}

function cleanText(value, maxLength = 255) {
  return String(value || '').replace(/\s+/g, ' ').trim().slice(0, maxLength);
}

function getClientIp(req) {
  const forwarded = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
  return forwarded || cleanText(req.socket?.remoteAddress || '', 128);
}

function getUserAgentFamily(userAgent) {
  const text = String(userAgent || '').toLowerCase();
  if (!text) return 'unknown';
  if (text.includes('edg/')) return 'edge';
  if (text.includes('chrome/')) return 'chrome';
  if (text.includes('firefox/')) return 'firefox';
  if (text.includes('safari/') && !text.includes('chrome/')) return 'safari';
  return 'other';
}

function getReferrerHost(value) {
  try {
    if (!value) return null;
    return new URL(String(value)).hostname.slice(0, 255) || null;
  } catch {
    return null;
  }
}

function buildFingerprint({ ip, userAgent, visitorId }) {
  const salt = process.env.ANALYTICS_FINGERPRINT_SALT || process.env.SUPABASE_SERVICE_ROLE_KEY || 'showtimes-nyc';
  const day = new Date().toISOString().slice(0, 10);
  return crypto.createHash('sha256').update([salt, day, ip, userAgent, visitorId].join('|')).digest('hex');
}

async function callSupabaseRpc(payload) {
  const response = await fetch(`${process.env.SUPABASE_URL}/rest/v1/rpc/record_site_visit`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      apikey: process.env.SUPABASE_SERVICE_ROLE_KEY,
      Authorization: `Bearer ${process.env.SUPABASE_SERVICE_ROLE_KEY}`,
    },
    body: JSON.stringify(payload),
  });

  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data?.message || `Supabase RPC failed (${response.status})`);
  }
  return data;
}

module.exports = async (req, res) => {
  if (req.method !== 'POST') {
    res.setHeader('Allow', 'POST');
    return respondJson(res, 405, { error: 'Method not allowed' });
  }

  if (!process.env.SUPABASE_URL || !process.env.SUPABASE_SERVICE_ROLE_KEY) {
    return respondJson(res, 503, { error: 'Analytics ingestion is not configured' });
  }

  try {
    const body = req.body && typeof req.body === 'object' ? req.body : {};
    const visitorId = cleanText(body.visitor_id, 128);
    const path = cleanText(body.path || '/', 255) || '/';
    const userId = cleanText(body.user_id, 64) || null;
    if (!visitorId) {
      return respondJson(res, 400, { error: 'visitor_id is required' });
    }

    const userAgent = cleanText(req.headers['user-agent'], 512);
    const result = await callSupabaseRpc({
      p_visitor_id: visitorId,
      p_user_id: userId,
      p_path: path,
      p_referrer_host: getReferrerHost(body.referrer),
      p_client_hint: getUserAgentFamily(userAgent),
      p_visit_fingerprint: buildFingerprint({
        ip: getClientIp(req),
        userAgent,
        visitorId,
      }),
    });

    return respondJson(res, 200, result);
  } catch (error) {
    return respondJson(res, 502, { error: error instanceof Error ? error.message : 'Analytics ingestion failed' });
  }
};
