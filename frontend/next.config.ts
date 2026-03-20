import type { NextConfig } from 'next'

const nextConfig: NextConfig = {
  output: 'standalone',
  env: {
    NEXT_PUBLIC_HUB_URL: process.env.NEXT_PUBLIC_HUB_URL || 'http://localhost:3002',
    NEXT_PUBLIC_ENGINE_URL: process.env.NEXT_PUBLIC_ENGINE_URL || 'http://localhost:8000',
  },
}

export default nextConfig
