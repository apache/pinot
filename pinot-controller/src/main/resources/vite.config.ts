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
  'query', 'version', 'debug', 'loggers',
];

// Match the prefix then require either '/', '?', or end-of-path.
// The old pattern used `$` which broke URLs with query strings like
// /tables?type=realtime — those never reached the backend proxy.
const proxyPattern = `^/(${BACKEND_API_PREFIXES.join('|')})([/?]|$)`;

export default defineConfig(({ mode }) => {
  const isProductionMode = mode === 'production';
  const runtimeEnv = isProductionMode ? 'production' : 'development';
  const apiProxyTarget = isProductionMode
    ? ''
    : process.env.VITE_API_PROXY_TARGET || 'http://localhost:9000';

  return {
    // Set the Vite project root to the app/ directory so index.html lives there
    root: path.join(__dirname, 'app'),
    base: './',

    // Static assets served from app/public/ (e.g. app/public/images/favicon.ico)
    publicDir: path.join(__dirname, 'app', 'public'),

    plugins: [
      react({
        // React 16 uses the classic JSX transform (no automatic runtime)
        jsxRuntime: 'classic',
      }),
    ],

    resolve: {
      alias: [
        {
          // Material UI v4 icon deep imports resolve to CommonJS entrypoints by default,
          // which Vite then treats as module objects instead of React components.
          find: /^@material-ui\/icons\/(.*)$/,
          replacement: '@material-ui/icons/esm/$1',
        },
        {
          // Map the bare 'Models' specifier used throughout the app to the
          // actual TypeScript module (replaces the old webpack resolve.modules trick)
          find: 'Models',
          replacement: path.join(__dirname, 'app', 'Models.ts'),
        },
      ],
    },

    define: {
      // Preserve process.env.NODE_ENV used in axios-config.ts
      'process.env.NODE_ENV': JSON.stringify(runtimeEnv),
      // Several CJS packages (react-codemirror2, etc.) reference Node.js `global`.
      // Browsers don't have it – map it to the standard globalThis.
      global: 'globalThis',
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
          target: apiProxyTarget,
          changeOrigin: true,
          secure: false, // Allow self-signed certificates in development
          rewrite: (p) => p,
          configure: (proxy) => {
            proxy.on('error', (err) => {
              console.error('[Proxy Error]', err);
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
        'react-spring',
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
        'prop-types', // Required for Material-UI
        'zustand',
        'zustand/shallow',
        'classcat',
        'd3-zoom',
        'd3-selection',
        'd3-drag',
        'react-refresh',
      ],
      // Exclude problematic packages from pre-bundling if needed
      exclude: ['system'],
      esbuildOptions: {
        mainFields: ['module', 'main'],
      },
    },
  };
});
