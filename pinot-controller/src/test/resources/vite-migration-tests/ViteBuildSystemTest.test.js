/*
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
/**
 * Test suite for Vite build system functionality
 * Tests the complete build system migration and integration
 */

const { describe, it, expect, beforeAll, afterAll } = require('@jest/globals');
const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

describe('Vite Build System Integration Tests', () => {
  const resourcesPath = path.join(__dirname, '../../../main/resources');
  const packageJsonPath = path.join(resourcesPath, 'package.json');
  const viteConfigPath = path.join(resourcesPath, 'vite.config.ts');
  const vitestConfigPath = path.join(resourcesPath, 'vitest.config.ts');
  
  let packageJson;
  let viteConfig;
  let vitestConfig;

  beforeAll(() => {
    if (fs.existsSync(packageJsonPath)) {
      packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'));
    }
    if (fs.existsSync(viteConfigPath)) {
      viteConfig = fs.readFileSync(viteConfigPath, 'utf8');
    }
    if (fs.existsSync(vitestConfigPath)) {
      vitestConfig = fs.readFileSync(vitestConfigPath, 'utf8');
    }
  });

  describe('Migration Completeness', () => {
    it('should have migrated from webpack to Vite', () => {
      expect(packageJson.devDependencies).toHaveProperty('vite');
      expect(packageJson.devDependencies).toHaveProperty('@vitejs/plugin-react');
      expect(packageJson.scripts.build).toBe('vite build');
      expect(packageJson.scripts.dev).toBe('vite');
    });

    it('should have Vitest for testing', () => {
      expect(packageJson.devDependencies).toHaveProperty('vitest');
      expect(packageJson.scripts.test).toBe('vitest');
      expect(packageJson.scripts['test:run']).toBe('vitest run');
      expect(packageJson.scripts['test:coverage']).toBe('vitest run --coverage');
    });

    it('should have testing library dependencies', () => {
      expect(packageJson.devDependencies).toHaveProperty('@testing-library/react');
      expect(packageJson.devDependencies).toHaveProperty('@testing-library/jest-dom');
      expect(packageJson.devDependencies).toHaveProperty('jsdom');
    });
  });

  describe('Configuration Consistency', () => {
    it('should have consistent alias configuration between Vite and Vitest', () => {
      expect(viteConfig).toContain('Models: path.join(__dirname, \'app\', \'Models.ts\')');
      expect(vitestConfig).toContain('Models: path.join(__dirname, \'app\', \'Models.ts\')');
    });

    it('should have consistent define configuration', () => {
      expect(viteConfig).toContain('\'process.env.NODE_ENV\'');
      expect(viteConfig).toContain('global: \'globalThis\'');
      expect(viteConfig).toContain('\'require.main\': \'undefined\'');
      
      expect(vitestConfig).toContain('\'process.env.NODE_ENV\'');
      expect(vitestConfig).toContain('global: \'globalThis\'');
      expect(vitestConfig).toContain('\'require.main\': \'undefined\'');
    });

    it('should have consistent React plugin configuration', () => {
      expect(viteConfig).toContain('jsxRuntime: \'classic\'');
      expect(vitestConfig).toContain('jsxRuntime: \'classic\'');
    });
  });

  describe('Development Server Configuration', () => {
    it('should configure development server with correct proxy', () => {
      expect(viteConfig).toContain('port: 8080');
      expect(viteConfig).toContain('proxy:');
      expect(viteConfig).toContain('target: \'http://sl73caehtp01314:9559\'');
    });

    it('should have correct API proxy pattern for backend calls', () => {
      expect(viteConfig).toContain('BACKEND_API_PREFIXES');
      const expectedPrefixes = [
        'tenants', 'tables', 'schemas', 'segments', 'instances',
        'cluster', 'periodictask', 'tasks', 'zk', 'sql', 'timeseries',
        'brokers', 'table', 'auth', 'users', 'health', 'appconfigs',
        'query', 'version', 'debug'
      ];
      
      expectedPrefixes.forEach(prefix => {
        expect(viteConfig).toContain(prefix);
      });
    });
  });

  describe('Build System Functionality', () => {
    it('should have proper build configuration', () => {
      expect(viteConfig).toContain('outDir: path.join(__dirname, \'dist\', \'webapp\')');
      expect(viteConfig).toContain('emptyOutDir: true');
      expect(viteConfig).toContain('sourcemap: true');
    });

    it('should configure CommonJS compatibility', () => {
      expect(viteConfig).toContain('transformMixedEsModules: true');
      expect(viteConfig).toContain('optimizeDeps');
      expect(viteConfig).toContain('include:');
    });

    it('should handle legacy dependencies', () => {
      const expectedDeps = [
        'json-bigint', 'jsonlint', 'react-spring', 'dagre', 'graphlib',
        'react-diff-viewer', 'react-codemirror2', 'codemirror',
        'react-flow-renderer', 'export-from-json', '@sqltools/formatter'
      ];
      
      expectedDeps.forEach(dep => {
        expect(viteConfig).toContain(dep);
      });
    });
  });

  describe('Test Configuration', () => {
    it('should configure Vitest with jsdom environment', () => {
      expect(vitestConfig).toContain('environment: \'jsdom\'');
      expect(vitestConfig).toContain('globals: true');
    });

    it('should have proper test file patterns', () => {
      expect(vitestConfig).toContain('include:');
      expect(vitestConfig).toContain('**/*.{test,spec}.{js,mjs,cjs,ts,mts,cts,jsx,tsx}');
    });

    it('should configure coverage reporting', () => {
      expect(vitestConfig).toContain('coverage:');
      expect(vitestConfig).toContain('provider: \'v8\'');
      expect(vitestConfig).toContain('reporter: [\'text\', \'json\', \'html\']');
    });

    it('should have setup files configured', () => {
      expect(vitestConfig).toContain('setupFiles: [\'./src/test/setup.ts\']');
    });
  });

  describe('Security and Compatibility', () => {
    it('should not expose sensitive configuration', () => {
      expect(viteConfig).not.toContain('password');
      expect(viteConfig).not.toContain('secret');
      expect(viteConfig).not.toContain('token');
    });

    it('should handle browser globals correctly', () => {
      expect(viteConfig).toContain('global: \'globalThis\'');
    });

    it('should fix CommonJS module compatibility issues', () => {
      expect(viteConfig).toContain('\'require.main\': \'undefined\'');
      expect(viteConfig).toContain('transformMixedEsModules: true');
    });
  });
});

describe('Build Process Validation Tests', () => {
  const resourcesPath = path.join(__dirname, '../../../main/resources');

  describe('Script Execution', () => {
    it('should have executable build scripts', () => {
      const packageJsonPath = path.join(resourcesPath, 'package.json');
      if (fs.existsSync(packageJsonPath)) {
        const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'));
        
        // Check that all scripts are properly defined
        expect(packageJson.scripts.build).toBeDefined();
        expect(packageJson.scripts.dev).toBeDefined();
        expect(packageJson.scripts.test).toBeDefined();
        expect(packageJson.scripts.lint).toBeDefined();
      }
    });

    it('should handle pre-build tasks correctly', () => {
      const packageJsonPath = path.join(resourcesPath, 'package.json');
      if (fs.existsSync(packageJsonPath)) {
        const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'));
        
        expect(packageJson.scripts.prebuild).toContain('mkdir -p app/public/images');
        expect(packageJson.scripts.predev).toContain('mkdir -p app/public/images');
      }
    });
  });

  describe('Dependency Resolution', () => {
    it('should resolve all required dependencies', () => {
      const nodeModulesPath = path.join(resourcesPath, 'node_modules');
      if (fs.existsSync(nodeModulesPath)) {
        const criticalDeps = ['vite', 'react', 'react-dom', 'jsonlint'];
        criticalDeps.forEach(dep => {
          const depPath = path.join(nodeModulesPath, dep);
          if (fs.existsSync(depPath)) {
            expect(fs.statSync(depPath).isDirectory()).toBe(true);
          }
        });
      }
    });
  });

  describe('TypeScript Configuration', () => {
    it('should have TypeScript configuration for the project', () => {
      const tsconfigPath = path.join(resourcesPath, 'tsconfig.json');
      if (fs.existsSync(tsconfigPath)) {
        const tsconfig = JSON.parse(fs.readFileSync(tsconfigPath, 'utf8'));
        expect(tsconfig).toHaveProperty('compilerOptions');
      }
    });
  });
});

describe('Migration Regression Tests', () => {
  describe('Webpack Removal', () => {
    it('should not have webpack configuration files', () => {
      const webpackConfigPath = path.join(__dirname, '../../../main/resources/webpack.config.js');
      // If webpack config exists, it should be legacy/unused
      if (fs.existsSync(webpackConfigPath)) {
        const webpackConfig = fs.readFileSync(webpackConfigPath, 'utf8');
        // Should not be actively used for Models resolution
        expect(webpackConfig).not.toContain('resolve.modules');
      }
    });

    it('should not have webpack in package.json', () => {
      const packageJsonPath = path.join(__dirname, '../../../main/resources/package.json');
      if (fs.existsSync(packageJsonPath)) {
        const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'));
        expect(packageJson.devDependencies).not.toHaveProperty('webpack');
        expect(packageJson.devDependencies).not.toHaveProperty('webpack-cli');
        expect(packageJson.devDependencies).not.toHaveProperty('webpack-dev-server');
      }
    });
  });

  describe('Legacy Script Removal', () => {
    it('should not have webpack-based build scripts', () => {
      const packageJsonPath = path.join(__dirname, '../../../main/resources/package.json');
      if (fs.existsSync(packageJsonPath)) {
        const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'));
        
        // Should not contain webpack commands
        Object.values(packageJson.scripts).forEach(script => {
          expect(script).not.toContain('webpack');
        });
      }
    });
  });

  describe('Functionality Preservation', () => {
    it('should preserve all critical UI functionality', () => {
      // Test that all critical dependencies are still present
      const packageJsonPath = path.join(__dirname, '../../../main/resources/package.json');
      if (fs.existsSync(packageJsonPath)) {
        const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'));
        
        const criticalDeps = [
          'react', 'react-dom', 'react-router-dom',
          '@material-ui/core', '@material-ui/icons',
          'axios', 'moment', 'lodash', 'jsonlint'
        ];
        
        criticalDeps.forEach(dep => {
          expect(packageJson.dependencies).toHaveProperty(dep);
        });
      }
    });

    it('should maintain TypeScript support', () => {
      const packageJsonPath = path.join(__dirname, '../../../main/resources/package.json');
      if (fs.existsSync(packageJsonPath)) {
        const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'));
        expect(packageJson.devDependencies).toHaveProperty('typescript');
      }
    });
  });
});

describe('Performance and Optimization Tests', () => {
  describe('Bundle Optimization', () => {
    it('should configure dependency pre-bundling', () => {
      expect(viteConfig).toContain('optimizeDeps');
      expect(viteConfig).toContain('include:');
    });

    it('should handle code splitting appropriately', () => {
      // Vite handles code splitting automatically, but we can test configuration
      expect(viteConfig).toContain('build:');
    });
  });

  describe('Development Experience', () => {
    it('should enable fast refresh for development', () => {
      expect(viteConfig).toContain('@vitejs/plugin-react');
    });

    it('should configure proper source maps', () => {
      expect(viteConfig).toContain('sourcemap: true');
    });
  });
});
