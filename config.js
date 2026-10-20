// DJ v2 — no Firebase account or Firebase Cloud Storage needed.
// 1. Deploy signaling-worker/ with Cloudflare Workers.
// 2. Replace the URL below with your actual workers.dev HTTPS address.
// 3. Upload this file with the website to GitHub Pages.
window.DJ_CONFIG = {
  signalingUrl: 'https://dj-private-signaling.dj-chat-dhrub2026.workers.dev',
  // Default public STUN servers; for difficult networks, add your own TURN
  // server credentials here (TURN use may cost money).
iceServers : [
  {
    urls: "stun:stun.relay.metered.ca:80",
  },
  {
    urls: "turn:asia-east.relay.metered.ca:80",
    username: "27b5326b94344bc1b0bce49c",
    credential: "tDy3q0Jj+bDCathp",
  },
  {
    urls: "turn:asia-east.relay.metered.ca:80?transport=tcp",
    username: "27b5326b94344bc1b0bce49c",
    credential: "tDy3q0Jj+bDCathp",
  },
  {
    urls: "turn:asia-east.relay.metered.ca:443",
    username: "27b5326b94344bc1b0bce49c",
    credential: "tDy3q0Jj+bDCathp",
  },
  {
    urls: "turns:asia-east.relay.metered.ca:443?transport=tcp",
    username: "27b5326b94344bc1b0bce49c",
    credential: "tDy3q0Jj+bDCathp",
  }
  ]
};
