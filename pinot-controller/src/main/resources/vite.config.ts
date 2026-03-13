/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Environment-based configuration
const config = {
  development: {
    VITE_API_PROXY_TARGET: process.env.VITE_API_PROXY_TARGET || 'http://localhost:9000',
  },
  production: {
    VITE_API_PROXY_TARGET: '', // Production uses relative URLs
  },
};

const currentEnv = process.env.NODE_ENV || 'development';
const envConfig = config[currentEnv] || config.development;

// Pinot backend API path prefixes to proxy during development
const BACKEND_API_PREFIXES = [
  'tenants', 'tables', 'schemas', 'segments', 'instances',
  'cluster', 'periodictask', 'tasks', 'zk', 'sql', 'timeseries',
  'brokers', 'table', 'auth', 'users', 'health', 'appconfigs',
  'query', 'version', 'debug',
];

// Match the prefix then require either '/', '?', or end-of-path.
// The old pattern used `$` which broke URLs with query strings like
// /tables?type=realtime — those never reached the backend proxy.
const proxyPattern = `^/(${BACKEND_API_PREFIXES.join('|')})([/?]|$)`;

export default defineConfig({
  // Set the Vite project root to the app/ directory so index.html lives there
  root: path.join(__dirname, 'app'),
  base : './',

  // Static assets served from app/public/ (e.g. app/public/images/favicon.ico)
  publicDir: path.join(__dirname, 'app', 'public'),

  plugins: [
    react({
      // React 16 uses the classic JSX transform (no automatic runtime)
      jsxRuntime: 'classic',
    }),
  ],

  resolve: {
    alias: {
      // Map the bare 'Models' specifier used throughout the app to the
      // actual TypeScript module (replaces the old webpack resolve.modules trick)
      Models: path.join(__dirname, 'app', 'Models.ts'),
    },
  },

  define: {
    // Preserve process.env.NODE_ENV used in axios-config.ts
    'process.env.NODE_ENV': JSON.stringify(currentEnv),
    // Several CJS packages (react-codemirror2, etc.) reference Node.js `global`.
    // Browsers don't have it – map it to the standard globalThis.
    global: 'globalThis',
    // Fix jsonlint error: line 429 checks "require.main === module"
    'require.main': 'undefined',
  },

  server: {
    port: 8080,
    host: true, // Listen on all addresses including LAN
    strictPort: false, // Try next port if 8080 is busy
    open: false, // Don't auto-open browser
    cors: true, // Enable CORS
    proxy: {
      // Forward all Pinot backend API calls to the controller
      // Default: localhost:9000 (can override with VITE_API_PROXY_TARGET env var)
      [proxyPattern]: {
        target: envConfig.VITE_API_PROXY_TARGET,
        changeOrigin: true,
        secure: false, // Allow self-signed certificates in development
        rewrite: (p) => p,
        configure: (proxy, options) => {
          proxy.on('error', (err, req, res) => {
            console.log('[Proxy Error]', err);
          });
          proxy.on('proxyReq', (proxyReq, req, res) => {
            console.log('[Proxy Request]', req.method, req.url, '→', options.target + req.url);
          });
          proxy.on('proxyRes', (proxyRes, req, res) => {
            console.log('[Proxy Response]', proxyRes.statusCode, req.url);
          });
        },
      },
    },
  },

  build: {
    outDir: path.join(__dirname, 'dist', 'webapp'),
    emptyOutDir: true,
    sourcemap: true,
    commonjsOptions: {
      transformMixedEsModules: true,
      // Ensure CommonJS modules are properly handled
      include: [/node_modules/],
      defaultIsModuleExports: true,
    },
  },

  optimizeDeps: {
    // Pre-bundle CJS packages so they work correctly in Vite's ESM environment.
    // Older packages (pre-ESM era) that don't have "module" or "exports" fields
    // in their package.json need explicit inclusion to avoid import errors.
    include: [
      'json-bigint',
      'jsonlint',
      'react-spring',
      'dagre',
      'graphlib',  // Required dependency of dagre
      'react-diff-viewer',
      'react-codemirror2',
      'codemirror',
      'react-flow-renderer',
      'export-from-json',
      '@sqltools/formatter',
      'moment-timezone',
      'echarts-for-react',
      'echarts',
      'tslib',
      'lodash',
      'prop-types',  // Required for Material-UI
      'zustand',
      'zustand/shallow',
      'classcat',
      'd3-zoom',
      'd3-selection',
      'd3-drag',
      'react-refresh',
    ],
    // Exclude problematic packages from pre-bundling if needed
    exclude: [],
    // Force Vite to handle CommonJS interop correctly
    esbuildOptions: {
      mainFields: ['module', 'main'],
      // Mark "system" as external to fix jsonlint CLI code that shouldn't run in browser
      external: ['system'],
    },
  },
});
