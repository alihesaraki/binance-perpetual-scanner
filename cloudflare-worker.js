// OPTIONAL but recommended: deploy this as a free Cloudflare Worker to get a
// reliable, unrestricted route to Binance instead of relying on public CORS
// proxies (corsproxy.io / allorigins.win), which are shared, rate-limited,
// and can go down.
//
// SETUP (5 minutes, free):
// 1. Go to https://dash.cloudflare.com -> Workers & Pages -> Create -> Worker
// 2. Delete the default code and paste this file's contents in.
// 3. Click Deploy. You'll get a URL like:
//      https://binance-proxy.YOUR-SUBDOMAIN.workers.dev
// 4. Open app.js in this project and set:
//      const CUSTOM_PROXY_BASE='https://binance-proxy.YOUR-SUBDOMAIN.workers.dev';
// 5. Reload the scanner. It will now route through your own Worker first,
//    which runs on Cloudflare's global network (not your ISP/VPN), so
//    regional blocks and CORS issues on your local connection no longer
//    affect it.

export default {
  async fetch(request) {
    const url = new URL(request.url);
    const targetUrl = 'https://fapi.binance.com' + url.pathname + url.search;

    const upstream = await fetch(targetUrl, {
      method: request.method,
      headers: { 'Accept': 'application/json' },
    });

    const body = await upstream.text();

    return new Response(body, {
      status: upstream.status,
      headers: {
        'Content-Type': upstream.headers.get('Content-Type') || 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Cache-Control': 'no-store',
      },
    });
  },
};
