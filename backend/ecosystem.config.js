module.exports = {
  apps: [{
    name: "duplicate",
    script: "./venv/bin/python",
    args: "-m uvicorn backend.main:app --host 0.0.0.0 --port 8009",
    cwd: "/mnt/additional-disk/duplicate-filtering",
    env: {
      PYTHONPATH: "/mnt/additional-disk/duplicate-filtering",
      CUDA_VISIBLE_DEVICES: "0"
    },
    autorestart: true,
    watch: false,
    max_memory_restart: '2G'
  }]
};
