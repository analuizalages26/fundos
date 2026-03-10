// netlify/functions/fundos-cached.js
// GET /api/fundos-cached              → serve latest cache from Blobs
// GET /api/fundos-cached?date=YYYY-MM-DD → serve specific date snapshot
//
// Falls back to live computation if cache miss.

const { getStore } = require('@netlify/blobs');

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Content-Type': 'application/json; charset=utf-8',
  'Cache-Control': 'public, max-age=1800, stale-while-revalidate=86400',
};

exports.handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') return { statusCode: 200, headers: CORS, body: '' };

  const { date } = event.queryStringParameters || {};
  const key = date ? `snapshot-${date}` : 'latest';

  try {
    const store = getStore('fundos-cache');
    const entry = await store.get(key);

    if (entry) {
      return { statusCode: 200, headers: CORS, body: entry };
    }

    // Cache miss → redirect to live endpoint
    return {
      statusCode: 307,
      headers: {
        ...CORS,
        Location: date ? `/api/fundos?baseDate=${date}` : '/api/fundos',
      },
      body: JSON.stringify({ message: 'Cache miss, redirecting to live' }),
    };
  } catch (err) {
    return {
      statusCode: 302,
      headers: { ...CORS, Location: date ? `/api/fundos?baseDate=${date}` : '/api/fundos' },
      body: '',
    };
  }
};
