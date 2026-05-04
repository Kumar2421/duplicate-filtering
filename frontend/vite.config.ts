import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    allowedHosts: ['duplicate.tools.thefusionapps.com'],
    host: '0.0.0.0',
    port: 9002,
    proxy: {
      '/api': {
        target: 'http://localhost:8009',
        changeOrigin: true,
      },
      '/images': {
        target: 'http://localhost:8009',
        changeOrigin: true,
      },
    },
  },
})
