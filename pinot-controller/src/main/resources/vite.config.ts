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
    'process.env.NODE_ENV': JSON.stringify(process.env.NODE_ENV || 'production'),
    // Several CJS packages (react-codemirror2, etc.) reference Node.js `global`.
    // Browsers don't have it – map it to the standard globalThis.
    global: 'globalThis',
    // Fix jsonlint error: line 429 checks "require.main === module"
    'require.main': 'undefined',
  },

  server: {
    port: 8080,
    proxy: {
      // Forward all Pinot backend API calls to the controller on port 9000
      [proxyPattern]: {
        target: 'http://sl73caehtp01314:9559',
        changeOrigin: true,
        rewrite: (p) => p,
      },
    },
  },

  build: {
    outDir: path.join(__dirname, 'dist', 'webapp'),
    emptyOutDir: true,
    sourcemap: true,
    commonjsOptions: {
      transformMixedEsModules: true,
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
    ],
  },
});
