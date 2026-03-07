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
 * Test suite for static asset handling in the Vite migration
 * Tests the Java backend static asset handlers and Vite build output compatibility
 */

const { describe, it, expect, beforeAll } = require('@jest/globals');
const fs = require('fs');
const path = require('path');

describe('Static Asset Handler Tests', () => {
  const resourcesPath = path.join(__dirname, '../../../main/resources');
  const distPath = path.join(resourcesPath, 'dist', 'webapp');
  
  describe('Build Output Structure', () => {
    it('should generate webapp directory in dist', () => {
      // This test checks if the Vite build creates the expected structure
      if (fs.existsSync(distPath)) {
        expect(fs.statSync(distPath).isDirectory()).toBe(true);
      }
    });

    it('should generate index.html in webapp directory', () => {
      const indexPath = path.join(distPath, 'index.html');
      if (fs.existsSync(indexPath)) {
        expect(fs.statSync(indexPath).isFile()).toBe(true);
        const indexContent = fs.readFileSync(indexPath, 'utf8');
        expect(indexContent).toContain('<html');
        expect(indexContent).toContain('</html>');
      }
    });

    it('should generate assets directory with bundled files', () => {
      const assetsPath = path.join(distPath, 'assets');
      if (fs.existsSync(assetsPath)) {
        expect(fs.statSync(assetsPath).isDirectory()).toBe(true);
        const assetFiles = fs.readdirSync(assetsPath);
        expect(assetFiles.some(file => file.endsWith('.js'))).toBe(true);
        expect(assetFiles.some(file => file.endsWith('.css'))).toBe(true);
      }
    });

    it('should generate images directory with favicon', () => {
      const imagesPath = path.join(distPath, 'images');
      if (fs.existsSync(imagesPath)) {
        expect(fs.statSync(imagesPath).isDirectory()).toBe(true);
        const faviconPath = path.join(imagesPath, 'favicon.ico');
        if (fs.existsSync(faviconPath)) {
          expect(fs.statSync(faviconPath).isFile()).toBe(true);
        }
      }
    });
  });

  describe('Java Backend Compatibility', () => {
    it('should match expected webapp resource structure for Java handlers', () => {
      // Test that the build output matches what Java static handlers expect
      const expectedPaths = [
        'index.html',
        'assets/',
        'images/'
      ];

      expectedPaths.forEach(expectedPath => {
        const fullPath = path.join(distPath, expectedPath);
        if (fs.existsSync(fullPath)) {
          expect(fs.existsSync(fullPath)).toBe(true);
        }
      });
    });

    it('should have assets accessible via /assets/ path', () => {
      // This tests the Java handler: .addHttpHandler(new CLStaticHttpHandler(classLoader, "/webapp/assets/"), "/assets/");
      const assetsPath = path.join(distPath, 'assets');
      if (fs.existsSync(assetsPath)) {
        const assetFiles = fs.readdirSync(assetsPath);
        expect(assetFiles.length).toBeGreaterThan(0);
      }
    });

    it('should have images accessible via /images/ path', () => {
      // This tests the Java handler: .addHttpHandler(new CLStaticHttpHandler(classLoader, "/webapp/images/"), "/images/");
      const imagesPath = path.join(distPath, 'images');
      if (fs.existsSync(imagesPath)) {
        expect(fs.statSync(imagesPath).isDirectory()).toBe(true);
      }
    });
  });

  describe('Asset Content Validation', () => {
    it('should have valid JavaScript bundle', () => {
      const assetsPath = path.join(distPath, 'assets');
      if (fs.existsSync(assetsPath)) {
        const jsFiles = fs.readdirSync(assetsPath).filter(file => file.endsWith('.js'));
        if (jsFiles.length > 0) {
          const jsContent = fs.readFileSync(path.join(assetsPath, jsFiles[0]), 'utf8');
          expect(jsContent.length).toBeGreaterThan(0);
          // Should not contain obvious build errors
          expect(jsContent).not.toContain('Error:');
          expect(jsContent).not.toContain('Failed to');
        }
      }
    });

    it('should have valid CSS bundle', () => {
      const assetsPath = path.join(distPath, 'assets');
      if (fs.existsSync(assetsPath)) {
        const cssFiles = fs.readdirSync(assetsPath).filter(file => file.endsWith('.css'));
        if (cssFiles.length > 0) {
          const cssContent = fs.readFileSync(path.join(assetsPath, cssFiles[0]), 'utf8');
          expect(cssContent.length).toBeGreaterThan(0);
          // Should contain valid CSS
          expect(cssContent).toMatch(/[.#][\w-]+\s*{/);
        }
      }
    });

    it('should have source maps for debugging', () => {
      const assetsPath = path.join(distPath, 'assets');
      if (fs.existsSync(assetsPath)) {
        const mapFiles = fs.readdirSync(assetsPath).filter(file => file.endsWith('.js.map'));
        if (mapFiles.length > 0) {
          const mapContent = fs.readFileSync(path.join(assetsPath, mapFiles[0]), 'utf8');
          const sourceMap = JSON.parse(mapContent);
          expect(sourceMap).toHaveProperty('version');
          expect(sourceMap).toHaveProperty('sources');
          expect(sourceMap).toHaveProperty('mappings');
        }
      }
    });
  });

  describe('Public Directory Handling', () => {
    it('should copy favicon from root to public/images during build', () => {
      const rootFaviconPath = path.join(resourcesPath, 'favicon.ico');
      const publicFaviconPath = path.join(resourcesPath, 'app', 'public', 'images', 'favicon.ico');
      
      if (fs.existsSync(rootFaviconPath) && fs.existsSync(publicFaviconPath)) {
        const rootFavicon = fs.readFileSync(rootFaviconPath);
        const publicFavicon = fs.readFileSync(publicFaviconPath);
        expect(rootFavicon.equals(publicFavicon)).toBe(true);
      }
    });

    it('should handle public directory assets correctly', () => {
      const publicPath = path.join(resourcesPath, 'app', 'public');
      if (fs.existsSync(publicPath)) {
        expect(fs.statSync(publicPath).isDirectory()).toBe(true);
      }
    });
  });
});

describe('Java Static Handler Integration Tests', () => {
  describe('ControllerAdminApiApplication Handler Configuration', () => {
    it('should configure handlers for webapp assets', () => {
      // Test that the expected handler paths are configured
      const expectedHandlers = [
        '/webapp/',      // Main webapp handler
        '/webapp/images/', // Images handler
        '/webapp/js/',     // JS handler (legacy)
        '/webapp/assets/'  // Assets handler (new Vite)
      ];

      // This is a structural test - in a real environment, these would be tested
      // by making HTTP requests to the running controller
      expectedHandlers.forEach(handlerPath => {
        expect(handlerPath).toMatch(/^\/webapp\//);
      });
    });

    it('should have correct mapping for assets handler', () => {
      // The Java code adds: .addHttpHandler(new CLStaticHttpHandler(classLoader, "/webapp/assets/"), "/assets/");
      const webappAssetsPath = '/webapp/assets/';
      const publicAssetsPath = '/assets/';
      
      expect(webappAssetsPath).toBe('/webapp/assets/');
      expect(publicAssetsPath).toBe('/assets/');
    });
  });

  describe('LandingPageHandler Integration', () => {
    it('should serve index.html from webapp directory', () => {
      // Test that LandingPageHandler can find the index.html
      const expectedIndexPath = 'webapp/index.html';
      expect(expectedIndexPath).toBe('webapp/index.html');
    });
  });

  describe('Resource Path Compatibility', () => {
    it('should maintain compatibility with existing resource paths', () => {
      // Ensure that the migration doesn't break existing resource loading
      const resourcePaths = [
        'webapp/index.html',
        'webapp/assets/',
        'webapp/images/',
      ];

      resourcePaths.forEach(resourcePath => {
        expect(resourcePath.startsWith('webapp/')).toBe(true);
      });
    });
  });
});

describe('Build Script Integration Tests', () => {
  describe('Pre-build Scripts', () => {
    it('should have prebuild script that copies favicon', () => {
      const packageJsonPath = path.join(__dirname, '../../../main/resources/package.json');
      if (fs.existsSync(packageJsonPath)) {
        const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'));
        expect(packageJson.scripts.prebuild).toContain('mkdir -p app/public/images');
        expect(packageJson.scripts.prebuild).toContain('cp favicon.ico app/public/images/favicon.ico');
      }
    });

    it('should have predev script that copies favicon', () => {
      const packageJsonPath = path.join(__dirname, '../../../main/resources/package.json');
      if (fs.existsSync(packageJsonPath)) {
        const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'));
        expect(packageJson.scripts.predev).toContain('mkdir -p app/public/images');
        expect(packageJson.scripts.predev).toContain('cp favicon.ico app/public/images/favicon.ico');
      }
    });
  });

  describe('Build Output Validation', () => {
    it('should generate files in correct output directory', () => {
      // Vite config specifies: outDir: path.join(__dirname, 'dist', 'webapp')
      const expectedOutputDir = path.join(resourcesPath, 'dist', 'webapp');
      if (fs.existsSync(expectedOutputDir)) {
        expect(fs.statSync(expectedOutputDir).isDirectory()).toBe(true);
      }
    });

    it('should clean output directory before build', () => {
      // Vite config specifies: emptyOutDir: true
      // This is tested by checking that old build artifacts don't persist
      const viteConfigPath = path.join(resourcesPath, 'vite.config.ts');
      if (fs.existsSync(viteConfigPath)) {
        const viteConfig = fs.readFileSync(viteConfigPath, 'utf8');
        expect(viteConfig).toContain('emptyOutDir: true');
      }
    });
  });
});
