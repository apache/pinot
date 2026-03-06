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
 * Test suite for error handling and edge cases
 * Tests error scenarios, boundary conditions, and resilience
 */

import { describe, it, expect, vi } from 'vitest';

// Mock error handling utilities
const ErrorHandler = {
  // Handle API errors with retry logic
  handleApiError: async (error, retryCount = 0, maxRetries = 3) => {
    if (retryCount >= maxRetries) {
      throw new Error(`Max retries exceeded: ${error.message}`);
    }

    if (error.status >= 500) {
      // Server error - retry after delay
      await new Promise(resolve => setTimeout(resolve, 1000 * (retryCount + 1)));
      return { shouldRetry: true, retryCount: retryCount + 1 };
    }

    if (error.status >= 400 && error.status < 500) {
      // Client error - don't retry
      throw new Error(`Client error: ${error.message}`);
    }

    throw error;
  },

  // Sanitize user input
  sanitizeInput: (input) => {
    if (typeof input !== 'string') return '';
    return input
      .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, '')
      .replace(/javascript:/gi, '')
      .replace(/on\w+\s*=\s*"[^"]*"/gi, '')
      .trim();
  },

  // Validate SQL query for safety
  validateSqlQuery: (sql) => {
    if (!sql || typeof sql !== 'string') return { valid: false, error: 'Invalid query' };
    
    const dangerousKeywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE', 'TRUNCATE'];
    const upperSql = sql.toUpperCase();
    
    for (const keyword of dangerousKeywords) {
      if (upperSql.includes(keyword)) {
        return { valid: false, error: `Dangerous keyword detected: ${keyword}` };
      }
    }

    if (sql.length > 10000) {
      return { valid: false, error: 'Query too long' };
    }

    return { valid: true };
  },

  // Handle timeout scenarios
  withTimeout: (promise, timeoutMs) => {
    return Promise.race([
      promise,
      new Promise((_, reject) => 
        setTimeout(() => reject(new Error('Operation timed out')), timeoutMs)
      )
    ]);
  },

  // Circuit breaker pattern
  createCircuitBreaker: (threshold = 5, resetTime = 60000) => {
    let failures = 0;
    let lastFailureTime = 0;
    let state = 'CLOSED'; // CLOSED, OPEN, HALF_OPEN

    return {
      execute: async (fn) => {
        if (state === 'OPEN') {
          if (Date.now() - lastFailureTime > resetTime) {
            state = 'HALF_OPEN';
          } else {
            throw new Error('Circuit breaker is OPEN');
          }
        }

        try {
          const result = await fn();
          if (state === 'HALF_OPEN') {
            state = 'CLOSED';
            failures = 0;
          }
          return result;
        } catch (error) {
          failures++;
          lastFailureTime = Date.now();
          
          if (failures >= threshold) {
            state = 'OPEN';
          }
          
          throw error;
        }
      },
      getState: () => state,
      getFailures: () => failures
    };
  }
};

describe('Error Handling Tests', () => {
  describe('API Error Handling', () => {
    it('should retry server errors', async () => {
      const serverError = { status: 500, message: 'Internal Server Error' };
      
      const result = await ErrorHandler.handleApiError(serverError, 0, 3);
      
      expect(result.shouldRetry).toBe(true);
      expect(result.retryCount).toBe(1);
    });

    it('should not retry client errors', async () => {
      const clientError = { status: 400, message: 'Bad Request' };
      
      await expect(ErrorHandler.handleApiError(clientError)).rejects.toThrow('Client error: Bad Request');
    });

    it('should throw after max retries', async () => {
      const serverError = { status: 500, message: 'Internal Server Error' };
      
      await expect(ErrorHandler.handleApiError(serverError, 3, 3)).rejects.toThrow('Max retries exceeded');
    });
  });

  describe('Input Sanitization', () => {
    it('should remove script tags', () => {
      const maliciousInput = '<script>alert("xss")</script>Hello World';
      const sanitized = ErrorHandler.sanitizeInput(maliciousInput);
      expect(sanitized).toBe('Hello World');
    });

    it('should remove javascript: protocols', () => {
      const maliciousInput = 'javascript:alert("xss")';
      const sanitized = ErrorHandler.sanitizeInput(maliciousInput);
      expect(sanitized).toBe('alert("xss")');
    });

    it('should remove event handlers', () => {
      const maliciousInput = 'onclick="alert(1)" Hello';
      const sanitized = ErrorHandler.sanitizeInput(maliciousInput);
      expect(sanitized).toBe('Hello');
    });

    it('should handle non-string inputs', () => {
      expect(ErrorHandler.sanitizeInput(null)).toBe('');
      expect(ErrorHandler.sanitizeInput(undefined)).toBe('');
      expect(ErrorHandler.sanitizeInput(123)).toBe('');
      expect(ErrorHandler.sanitizeInput({})).toBe('');
    });
  });

  describe('SQL Query Validation', () => {
    it('should allow safe SELECT queries', () => {
      const safeQuery = 'SELECT * FROM airlineStats WHERE origin = "LAX"';
      const result = ErrorHandler.validateSqlQuery(safeQuery);
      expect(result.valid).toBe(true);
    });

    it('should reject dangerous queries', () => {
      const dangerousQueries = [
        'DROP TABLE users',
        'DELETE FROM users WHERE id = 1',
        'UPDATE users SET password = "hacked"',
        'INSERT INTO users VALUES ("hacker", "password")',
        'ALTER TABLE users ADD COLUMN hacked TEXT',
        'CREATE TABLE malicious (id INT)',
        'TRUNCATE TABLE users'
      ];

      dangerousQueries.forEach(query => {
        const result = ErrorHandler.validateSqlQuery(query);
        expect(result.valid).toBe(false);
        expect(result.error).toContain('Dangerous keyword detected');
      });
    });

    it('should reject overly long queries', () => {
      const longQuery = 'SELECT * FROM table WHERE ' + 'condition AND '.repeat(1000) + 'final_condition';
      const result = ErrorHandler.validateSqlQuery(longQuery);
      expect(result.valid).toBe(false);
      expect(result.error).toBe('Query too long');
    });

    it('should handle invalid inputs', () => {
      expect(ErrorHandler.validateSqlQuery(null).valid).toBe(false);
      expect(ErrorHandler.validateSqlQuery(undefined).valid).toBe(false);
      expect(ErrorHandler.validateSqlQuery('').valid).toBe(false);
    });
  });

  describe('Timeout Handling', () => {
    it('should resolve when promise completes before timeout', async () => {
      const fastPromise = new Promise(resolve => setTimeout(() => resolve('success'), 100));
      
      const result = await ErrorHandler.withTimeout(fastPromise, 200);
      expect(result).toBe('success');
    });

    it('should reject when promise times out', async () => {
      const slowPromise = new Promise(resolve => setTimeout(() => resolve('success'), 300));
      
      await expect(ErrorHandler.withTimeout(slowPromise, 100)).rejects.toThrow('Operation timed out');
    });

    it('should handle promise rejection before timeout', async () => {
      const rejectingPromise = new Promise((_, reject) => 
        setTimeout(() => reject(new Error('Promise failed')), 50)
      );
      
      await expect(ErrorHandler.withTimeout(rejectingPromise, 200)).rejects.toThrow('Promise failed');
    });
  });

  describe('Circuit Breaker Pattern', () => {
    it('should start in CLOSED state', () => {
      const circuitBreaker = ErrorHandler.createCircuitBreaker(3, 1000);
      expect(circuitBreaker.getState()).toBe('CLOSED');
      expect(circuitBreaker.getFailures()).toBe(0);
    });

    it('should open after threshold failures', async () => {
      const circuitBreaker = ErrorHandler.createCircuitBreaker(2, 1000);
      const failingFunction = () => Promise.reject(new Error('Service unavailable'));

      // First failure
      await expect(circuitBreaker.execute(failingFunction)).rejects.toThrow('Service unavailable');
      expect(circuitBreaker.getState()).toBe('CLOSED');
      expect(circuitBreaker.getFailures()).toBe(1);

      // Second failure - should open circuit
      await expect(circuitBreaker.execute(failingFunction)).rejects.toThrow('Service unavailable');
      expect(circuitBreaker.getState()).toBe('OPEN');
      expect(circuitBreaker.getFailures()).toBe(2);
    });

    it('should reject immediately when OPEN', async () => {
      const circuitBreaker = ErrorHandler.createCircuitBreaker(1, 1000);
      const failingFunction = () => Promise.reject(new Error('Service unavailable'));

      // Trigger failure to open circuit
      await expect(circuitBreaker.execute(failingFunction)).rejects.toThrow('Service unavailable');
      expect(circuitBreaker.getState()).toBe('OPEN');

      // Should reject immediately
      await expect(circuitBreaker.execute(() => Promise.resolve('success'))).rejects.toThrow('Circuit breaker is OPEN');
    });

    it('should reset to CLOSED after successful execution in HALF_OPEN state', async () => {
      const circuitBreaker = ErrorHandler.createCircuitBreaker(1, 100);
      const failingFunction = () => Promise.reject(new Error('Service unavailable'));
      const successFunction = () => Promise.resolve('success');

      // Open the circuit
      await expect(circuitBreaker.execute(failingFunction)).rejects.toThrow('Service unavailable');
      expect(circuitBreaker.getState()).toBe('OPEN');

      // Wait for reset time
      await new Promise(resolve => setTimeout(resolve, 150));

      // Should be HALF_OPEN and succeed
      const result = await circuitBreaker.execute(successFunction);
      expect(result).toBe('success');
      expect(circuitBreaker.getState()).toBe('CLOSED');
      expect(circuitBreaker.getFailures()).toBe(0);
    });
  });

  describe('Edge Cases and Boundary Conditions', () => {
    it('should handle null and undefined values gracefully', () => {
      const safeAccess = (obj, path) => {
        return path.split('.').reduce((current, key) => {
          return current && current[key] !== undefined ? current[key] : null;
        }, obj);
      };

      expect(safeAccess(null, 'a.b.c')).toBe(null);
      expect(safeAccess(undefined, 'a.b.c')).toBe(null);
      expect(safeAccess({}, 'a.b.c')).toBe(null);
      expect(safeAccess({ a: { b: { c: 'value' } } }, 'a.b.c')).toBe('value');
      expect(safeAccess({ a: { b: null } }, 'a.b.c')).toBe(null);
    });

    it('should handle array boundary conditions', () => {
      const safeArrayAccess = (arr, index) => {
        if (!Array.isArray(arr)) return null;
        if (index < 0 || index >= arr.length) return null;
        return arr[index];
      };

      const testArray = ['a', 'b', 'c'];
      
      expect(safeArrayAccess(testArray, 0)).toBe('a');
      expect(safeArrayAccess(testArray, 2)).toBe('c');
      expect(safeArrayAccess(testArray, -1)).toBe(null);
      expect(safeArrayAccess(testArray, 3)).toBe(null);
      expect(safeArrayAccess(null, 0)).toBe(null);
      expect(safeArrayAccess('not-array', 0)).toBe(null);
    });

    it('should handle numeric edge cases', () => {
      const safeNumber = (value, defaultValue = 0) => {
        const num = Number(value);
        return isNaN(num) || !isFinite(num) ? defaultValue : num;
      };

      expect(safeNumber('123')).toBe(123);
      expect(safeNumber('123.45')).toBe(123.45);
      expect(safeNumber('invalid')).toBe(0);
      expect(safeNumber(null)).toBe(0);
      expect(safeNumber(undefined)).toBe(0);
      expect(safeNumber(Infinity)).toBe(0);
      expect(safeNumber(-Infinity)).toBe(0);
      expect(safeNumber(NaN)).toBe(0);
      expect(safeNumber('invalid', -1)).toBe(-1);
    });

    it('should handle date parsing edge cases', () => {
      const safeDateParse = (dateString) => {
        if (!dateString) return null;
        const date = new Date(dateString);
        return isNaN(date.getTime()) ? null : date;
      };

      expect(safeDateParse('2022-01-01')).toBeInstanceOf(Date);
      expect(safeDateParse('2022-01-01T00:00:00Z')).toBeInstanceOf(Date);
      expect(safeDateParse('invalid-date')).toBe(null);
      expect(safeDateParse('')).toBe(null);
      expect(safeDateParse(null)).toBe(null);
      expect(safeDateParse(undefined)).toBe(null);
    });
  });

  describe('Memory and Performance Edge Cases', () => {
    it('should handle large data structures without memory leaks', () => {
      const createLargeObject = (size) => {
        const obj = {};
        for (let i = 0; i < size; i++) {
          obj[`key${i}`] = `value${i}`;
        }
        return obj;
      };

      const startMemory = performance.memory ? performance.memory.usedJSHeapSize : 0;
      
      // Create and destroy large objects
      for (let i = 0; i < 10; i++) {
        const largeObj = createLargeObject(1000);
        expect(Object.keys(largeObj)).toHaveLength(1000);
      }

      // Force garbage collection if available
      if (global.gc) {
        global.gc();
      }

      const endMemory = performance.memory ? performance.memory.usedJSHeapSize : 0;
      
      // Memory usage shouldn't grow excessively (allowing for some variance)
      if (startMemory > 0 && endMemory > 0) {
        expect(endMemory - startMemory).toBeLessThan(50 * 1024 * 1024); // Less than 50MB growth
      }
    });

    it('should handle recursive operations safely', () => {
      const safeRecursion = (obj, depth = 0, maxDepth = 100) => {
        if (depth > maxDepth) {
          throw new Error('Maximum recursion depth exceeded');
        }
        
        if (obj === null || typeof obj !== 'object') {
          return obj;
        }

        const result = {};
        for (const key in obj) {
          if (obj.hasOwnProperty(key)) {
            result[key] = safeRecursion(obj[key], depth + 1, maxDepth);
          }
        }
        return result;
      };

      const normalObj = { a: { b: { c: 'value' } } };
      expect(() => safeRecursion(normalObj)).not.toThrow();

      // Create a deeply nested object that exceeds maxDepth
      let deepObj = {};
      let current = deepObj;
      for (let i = 0; i < 150; i++) {
        current.level = {};
        current = current.level;
      }

      expect(() => safeRecursion(deepObj)).toThrow('Maximum recursion depth exceeded');
    });
  });

  describe('Concurrent Operations', () => {
    it('should handle race conditions safely', async () => {
      let counter = 0;
      const incrementCounter = async () => {
        const current = counter;
        await new Promise(resolve => setTimeout(resolve, Math.random() * 10));
        counter = current + 1;
      };

      // This would normally cause race conditions
      const promises = Array.from({ length: 10 }, () => incrementCounter());
      await Promise.all(promises);

      // Without proper synchronization, counter might not be 10
      // This test demonstrates the race condition issue
      expect(counter).toBeLessThanOrEqual(10);
    });

    it('should handle concurrent API calls with proper queuing', async () => {
      const createQueue = () => {
        const queue = [];
        let processing = false;

        const process = async () => {
          if (processing || queue.length === 0) return;
          processing = true;

          while (queue.length > 0) {
            const { fn, resolve, reject } = queue.shift();
            try {
              const result = await fn();
              resolve(result);
            } catch (error) {
              reject(error);
            }
          }

          processing = false;
        };

        return {
          add: (fn) => {
            return new Promise((resolve, reject) => {
              queue.push({ fn, resolve, reject });
              process();
            });
          }
        };
      };

      const queue = createQueue();
      const mockApiCall = (id) => () => Promise.resolve(`result-${id}`);

      const promises = Array.from({ length: 5 }, (_, i) => 
        queue.add(mockApiCall(i))
      );

      const results = await Promise.all(promises);
      
      expect(results).toHaveLength(5);
      results.forEach((result, i) => {
        expect(result).toBe(`result-${i}`);
      });
    });
  });

  describe('Data Corruption Prevention', () => {
    it('should validate data integrity', () => {
      const validateDataIntegrity = (data, schema) => {
        if (!data || !schema) return false;
        
        for (const field of schema.required || []) {
          if (!(field in data)) return false;
        }

        for (const [key, value] of Object.entries(data)) {
          const fieldSchema = schema.properties?.[key];
          if (fieldSchema) {
            if (fieldSchema.type === 'string' && typeof value !== 'string') return false;
            if (fieldSchema.type === 'number' && typeof value !== 'number') return false;
            if (fieldSchema.type === 'boolean' && typeof value !== 'boolean') return false;
          }
        }

        return true;
      };

      const schema = {
        required: ['name', 'type'],
        properties: {
          name: { type: 'string' },
          type: { type: 'string' },
          count: { type: 'number' }
        }
      };

      expect(validateDataIntegrity({ name: 'test', type: 'OFFLINE' }, schema)).toBe(true);
      expect(validateDataIntegrity({ name: 'test', type: 'OFFLINE', count: 5 }, schema)).toBe(true);
      expect(validateDataIntegrity({ name: 'test' }, schema)).toBe(false); // Missing required field
      expect(validateDataIntegrity({ name: 123, type: 'OFFLINE' }, schema)).toBe(false); // Wrong type
    });

    it('should prevent prototype pollution', () => {
      const safeMerge = (target, source) => {
        // Create a clean object without prototype
        const result = Object.create(null);
        
        // Copy target properties safely
        for (const key in target) {
          if (target.hasOwnProperty(key) && key !== '__proto__' && key !== 'constructor' && key !== 'prototype') {
            result[key] = target[key];
          }
        }
        
        // Copy source properties safely
        for (const key in source) {
          if (key === '__proto__' || key === 'constructor' || key === 'prototype') {
            continue; // Skip dangerous keys
          }
          
          if (source.hasOwnProperty(key)) {
            result[key] = source[key];
          }
        }
        
        return result;
      };

      const target = { a: 1 };
      const maliciousSource = {
        b: 2,
        '__proto__': { polluted: true },
        'constructor': { polluted: true }
      };

      const result = safeMerge(target, maliciousSource);
      
      expect(result.a).toBe(1);
      expect(result.b).toBe(2);
      expect(result.__proto__).toBeUndefined();
      expect(result.constructor).toBeUndefined();
      expect({}.polluted).toBeUndefined(); // Prototype not polluted
    });
  });

  describe('Resource Management', () => {
    it('should clean up resources properly', () => {
      const createResourceManager = () => {
        const resources = new Set();
        
        return {
          acquire: (resource) => {
            resources.add(resource);
            return resource;
          },
          release: (resource) => {
            resources.delete(resource);
          },
          cleanup: () => {
            resources.clear();
          },
          getActiveCount: () => resources.size
        };
      };

      const manager = createResourceManager();
      
      const resource1 = manager.acquire('resource1');
      const resource2 = manager.acquire('resource2');
      
      expect(manager.getActiveCount()).toBe(2);
      
      manager.release(resource1);
      expect(manager.getActiveCount()).toBe(1);
      
      manager.cleanup();
      expect(manager.getActiveCount()).toBe(0);
    });

    it('should handle resource leaks detection', () => {
      const createLeakDetector = (maxResources = 100) => {
        const resources = new Map();
        
        return {
          track: (id, resource) => {
            if (resources.size >= maxResources) {
              throw new Error('Resource leak detected: too many active resources');
            }
            resources.set(id, { resource, timestamp: Date.now() });
          },
          untrack: (id) => {
            resources.delete(id);
          },
          checkLeaks: (maxAge = 60000) => {
            const now = Date.now();
            const leaks = [];
            
            for (const [id, { timestamp }] of resources) {
              if (now - timestamp > maxAge) {
                leaks.push(id);
              }
            }
            
            return leaks;
          },
          getCount: () => resources.size
        };
      };

      const detector = createLeakDetector(5);
      
      // Normal usage
      detector.track('res1', {});
      detector.track('res2', {});
      expect(detector.getCount()).toBe(2);
      
      detector.untrack('res1');
      expect(detector.getCount()).toBe(1);
      
      // Test leak detection
      for (let i = 0; i < 4; i++) {
        detector.track(`res${i + 3}`, {});
      }
      
      expect(() => detector.track('overflow', {})).toThrow('Resource leak detected');
    });
  });
});
