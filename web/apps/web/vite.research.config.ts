import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'
import path from 'node:path'

export default defineConfig({
  plugins: [react(), tailwindcss(), {
    name: 'research-build-boundary',
    generateBundle(_options, bundle) {
      const forbidden = /(?:lib\/(?:supabase|product-analytics|auth-session)\.ts|components\/auth-guard\.tsx|routes\/(?:login|remote)\.tsx)$/
      for (const output of Object.values(bundle)) {
        if (output.type !== 'chunk') continue
        if (Object.keys(output.modules).some(id => forbidden.test(id) || id.includes('/@supabase/') || id.includes('/@supabase+'))) this.error('本机研究构建不能引入云端登录、遥控或站点分析模块')
      }
    },
  }],
  publicDir: false,
  resolve: { alias: { '@': path.resolve(__dirname, './src') } },
  build: {
    outDir: 'dist-research',
    sourcemap: false,
    rollupOptions: { input: path.resolve(__dirname, 'research.html') },
  },
})
