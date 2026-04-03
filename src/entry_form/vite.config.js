import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 3001,
    /* Proxy /api calls to FastAPI when backend is ready.
       Change target to your Render URL in production. */
    proxy: {
      '/api': {
        target:      'http://localhost:8000',
        changeOrigin: true,
      }
    }
  }
})
