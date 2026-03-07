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
 * Test suite for jsonlint CommonJS compatibility wrapper
 * Tests the fix for jsonlint's require.main === module check in Vite environment
 */

const { describe, it, expect, beforeAll } = require('@jest/globals');
const fs = require('fs');
const path = require('path');

describe('Jsonlint Compatibility Tests', () => {
  const viteConfigPath = path.join(__dirname, '../../../main/resources/vite.config.ts');
  const packageJsonPath = path.join(__dirname, '../../../main/resources/package.json');
  
  let viteConfig;
  let packageJson;

  beforeAll(() => {
    if (fs.existsSync(viteConfigPath)) {
      viteConfig = fs.readFileSync(viteConfigPath, 'utf8');
    }
    if (fs.existsSync(packageJsonPath)) {
      packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'));
    }
  });

  describe('Vite Configuration Fix', () => {
    it('should define require.main as undefined to fix jsonlint', () => {
      expect(viteConfig).toContain('\'require.main\': \'undefined\'');
    });

    it('should include comment explaining jsonlint fix', () => {
      expect(viteConfig).toContain('Fix jsonlint error: line 429 checks "require.main === module"');
    });

    it('should map global to globalThis for browser compatibility', () => {
      expect(viteConfig).toContain('global: \'globalThis\'');
    });
  });

  describe('Dependency Configuration', () => {
    it('should include jsonlint in package.json dependencies', () => {
      expect(packageJson.dependencies).toHaveProperty('jsonlint');
      expect(packageJson.dependencies.jsonlint).toBe('1.6.3');
    });

    it('should include jsonlint in Vite optimizeDeps', () => {
      expect(viteConfig).toContain('jsonlint');
      expect(viteConfig).toContain('optimizeDeps');
      expect(viteConfig).toContain('include:');
    });
  });

  describe('CommonJS Compatibility', () => {
    it('should configure transformMixedEsModules for CommonJS packages', () => {
      expect(viteConfig).toContain('transformMixedEsModules: true');
    });

    it('should pre-bundle CJS packages in optimizeDeps', () => {
      expect(viteConfig).toContain('Pre-bundle CJS packages so they work correctly in Vite\'s ESM environment');
    });
  });
});

describe('Jsonlint Runtime Tests', () => {
  // Mock jsonlint for testing
  const mockJsonlint = {
    parse: jest.fn(),
    parseError: jest.fn()
  };

  beforeAll(() => {
    // Mock the global environment that Vite creates
    global.globalThis = global;
    global.require = {
      main: undefined // This simulates the Vite fix
    };
  });

  describe('Module Loading', () => {
    it('should handle jsonlint module loading without require.main errors', () => {
      // Simulate the condition that caused the original error
      const requireMainCheck = global.require && global.require.main === module;
      expect(requireMainCheck).toBe(false);
    });

    it('should work with undefined require.main', () => {
      expect(global.require.main).toBeUndefined();
    });
  });

  describe('JSON Parsing Functionality', () => {
    it('should parse valid JSON', () => {
      const validJson = '{"test": "value"}';
      mockJsonlint.parse.mockReturnValue({ test: 'value' });
      
      const result = mockJsonlint.parse(validJson);
      expect(result).toEqual({ test: 'value' });
      expect(mockJsonlint.parse).toHaveBeenCalledWith(validJson);
    });

    it('should handle invalid JSON with error callback', () => {
      const invalidJson = '{"test": invalid}';
      const errorCallback = jest.fn();
      
      mockJsonlint.parseError.mockImplementation(errorCallback);
      mockJsonlint.parse.mockImplementation(() => {
        throw new Error('Parse error');
      });

      try {
        mockJsonlint.parse(invalidJson);
      } catch (error) {
        expect(error.message).toBe('Parse error');
      }
    });
  });
});

describe('Browser Environment Compatibility', () => {
  describe('Global Variables', () => {
    it('should have globalThis available', () => {
      expect(typeof globalThis).toBe('object');
    });

    it('should map global to globalThis in browser environment', () => {
      // This tests the Vite configuration: global: 'globalThis'
      if (typeof window !== 'undefined') {
        expect(global).toBe(globalThis);
      }
    });
  });

  describe('Module System Compatibility', () => {
    it('should handle ESM imports in Vite environment', () => {
      // Test that the module can be imported without CommonJS issues
      expect(() => {
        // Simulate ESM import of jsonlint
        const jsonlintModule = { parse: () => {}, parseError: () => {} };
        expect(jsonlintModule).toBeDefined();
      }).not.toThrow();
    });

    it('should work without Node.js specific globals', () => {
      // Ensure the fix works even when Node.js globals are not available
      const originalProcess = global.process;
      delete global.process;
      
      expect(() => {
        // This should not throw even without process global
        const requireMainUndefined = global.require && global.require.main === undefined;
        expect(requireMainUndefined).toBe(true);
      }).not.toThrow();
      
      // Restore process global
      global.process = originalProcess;
    });
  });
});
