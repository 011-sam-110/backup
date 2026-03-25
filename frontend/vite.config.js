import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    allowedHosts: ['supersweet-unbeauteous-noma.ngrok-free.dev'],
    proxy: {
      '/api': 'http://localhost:8000',
    },
  },
})
