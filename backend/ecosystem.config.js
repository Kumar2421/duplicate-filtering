module.exports = {
  apps: [{
    name: "duplicate",
    script: "./venv/bin/python",
    args: "-m uvicorn backend.main:app --host 0.0.0.0 --port 8009",
    cwd: "/mnt/additional-disk/duplicate-filtering",
    env: {
      PYTHONPATH: "/mnt/additional-disk/duplicate-filtering",
      CUDA_VISIBLE_DEVICES: "0",

      // Option B (branch-wise token fetching) - provide these via shell env or PM2 ecosystem
      // so they are NOT hardcoded in the repo.
      ANALYTICS_EMAIL: process.env.ANALYTICS_EMAIL,
      ANALYTICS_PASSWORD: process.env.ANALYTICS_PASSWORD,
      ANALYTICS_DEVICE_ID: process.env.ANALYTICS_DEVICE_ID,

      // Optional overrides (defaults are already set in AnalyticsAuthService)
      ANALYTICS_LOGIN_URL: process.env.ANALYTICS_LOGIN_URL,
      ANALYTICS_BRANCH_SWITCH_URL: process.env.ANALYTICS_BRANCH_SWITCH_URL,
      ANALYTICS_ORIGIN: process.env.ANALYTICS_ORIGIN,
      ANALYTICS_REFERER: process.env.ANALYTICS_REFERER,
      ANALYTICS_ACCEPT_LANGUAGE: process.env.ANALYTICS_ACCEPT_LANGUAGE,
    },
    autorestart: true,
    watch: false,
    max_memory_restart: '4G'
  }]
};
