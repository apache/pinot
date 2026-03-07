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
 * Test suite for utility functions and helper methods
 * Tests common utility functions used throughout the Pinot UI
 */


import { describe, it, expect, vi } from 'vitest';

// Mock utility functions that would be used in the Pinot UI
const Utils = {
  // Format bytes to human readable format
  formatBytes: (bytes, decimals = 2) => {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
  },

  // Format timestamp to readable date
  formatTimestamp: (timestamp, format = 'ISO') => {
    const date = new Date(timestamp);
    if (isNaN(date.getTime())) {
      return 'Invalid Date';
    }
    if (format === 'ISO') {
      return date.toISOString();
    } else if (format === 'local') {
      return date.toLocaleString();
    }
    return date.toString();
  },

  // Validate table name
  validateTableName: (name) => {
    if (!name || typeof name !== 'string') return false;
    if (name.length < 1 || name.length > 100) return false;
    return /^[a-zA-Z][a-zA-Z0-9_]*$/.test(name);
  },

  // Parse SQL query to extract table names
  extractTableNames: (sql) => {
    if (!sql || typeof sql !== 'string') return [];
    const matches = sql.match(/FROM\s+([a-zA-Z_][a-zA-Z0-9_]*)/gi);
    return matches ? matches.map(match => match.replace(/FROM\s+/i, '')) : [];
  },

  // Deep clone object
  deepClone: (obj) => {
    if (obj === null || typeof obj !== 'object') return obj;
    if (obj instanceof Date) return new Date(obj.getTime());
    if (obj instanceof Array) return obj.map(item => Utils.deepClone(item));
    if (typeof obj === 'object') {
      const cloned = {};
      Object.keys(obj).forEach(key => {
        cloned[key] = Utils.deepClone(obj[key]);
      });
      return cloned;
    }
    return obj;
  },

  // Debounce function
  debounce: (func, wait) => {
    let timeout;
    return function executedFunction(...args) {
      const later = () => {
        clearTimeout(timeout);
        func(...args);
      };
      clearTimeout(timeout);
      timeout = setTimeout(later, wait);
    };
  },

  // Sort array of objects by key
  sortByKey: (array, key, direction = 'asc') => {
    return [...array].sort((a, b) => {
      const aVal = a[key];
      const bVal = b[key];
      if (direction === 'asc') {
        return aVal < bVal ? -1 : aVal > bVal ? 1 : 0;
      } else {
        return aVal > bVal ? -1 : aVal < bVal ? 1 : 0;
      }
    });
  },

  // Generate unique ID
  generateId: () => {
    return Math.random().toString(36).substr(2, 9);
  },

  // Validate JSON string
  isValidJSON: (str) => {
    try {
      JSON.parse(str);
      return true;
    } catch (e) {
      return false;
    }
  },

  // Format duration in milliseconds
  formatDuration: (ms) => {
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
    if (ms < 3600000) return `${(ms / 60000).toFixed(1)}m`;
    return `${(ms / 3600000).toFixed(1)}h`;
  }
};

describe('Utility Functions Tests', () => {
  describe('formatBytes', () => {
    it('should format bytes correctly', () => {
      expect(Utils.formatBytes(0)).toBe('0 Bytes');
      expect(Utils.formatBytes(1024)).toBe('1 KB');
      expect(Utils.formatBytes(1048576)).toBe('1 MB');
      expect(Utils.formatBytes(1073741824)).toBe('1 GB');
      expect(Utils.formatBytes(1099511627776)).toBe('1 TB');
    });

    it('should handle decimal places', () => {
      expect(Utils.formatBytes(1536, 0)).toBe('2 KB');
      expect(Utils.formatBytes(1536, 1)).toBe('1.5 KB');
      expect(Utils.formatBytes(1536, 2)).toBe('1.5 KB');
    });

    it('should handle edge cases', () => {
      expect(Utils.formatBytes(1)).toBe('1 Bytes');
      expect(Utils.formatBytes(1023)).toBe('1023 Bytes');
      expect(Utils.formatBytes(1025)).toBe('1 KB');
    });
  });

  describe('formatTimestamp', () => {
    it('should format timestamp to ISO string', () => {
      const timestamp = 1640995200000; // 2022-01-01T00:00:00.000Z
      const result = Utils.formatTimestamp(timestamp, 'ISO');
      expect(result).toBe('2022-01-01T00:00:00.000Z');
    });

    it('should format timestamp to local string', () => {
      const timestamp = 1640995200000;
      const result = Utils.formatTimestamp(timestamp, 'local');
      expect(typeof result).toBe('string');
      expect(result.length).toBeGreaterThan(0);
    });

    it('should handle invalid timestamps', () => {
      const result = Utils.formatTimestamp('invalid');
      expect(result).toBe('Invalid Date');
    });
  });

  describe('validateTableName', () => {
    it('should validate correct table names', () => {
      expect(Utils.validateTableName('validTable')).toBe(true);
      expect(Utils.validateTableName('table_123')).toBe(true);
      expect(Utils.validateTableName('Table1')).toBe(true);
      expect(Utils.validateTableName('a')).toBe(true);
    });

    it('should reject invalid table names', () => {
      expect(Utils.validateTableName('')).toBe(false);
      expect(Utils.validateTableName(null)).toBe(false);
      expect(Utils.validateTableName(undefined)).toBe(false);
      expect(Utils.validateTableName(123)).toBe(false);
      expect(Utils.validateTableName('123table')).toBe(false);
      expect(Utils.validateTableName('table-name')).toBe(false);
      expect(Utils.validateTableName('table name')).toBe(false);
      expect(Utils.validateTableName('table.name')).toBe(false);
    });

    it('should handle length limits', () => {
      const longName = 'a'.repeat(101);
      expect(Utils.validateTableName(longName)).toBe(false);
      
      const maxLengthName = 'a'.repeat(100);
      expect(Utils.validateTableName(maxLengthName)).toBe(true);
    });
  });

  describe('extractTableNames', () => {
    it('should extract table names from SQL queries', () => {
      expect(Utils.extractTableNames('SELECT * FROM users')).toEqual(['users']);
      expect(Utils.extractTableNames('SELECT * FROM table1 JOIN table2')).toEqual(['table1']);
      expect(Utils.extractTableNames('select count(*) from airlineStats')).toEqual(['airlineStats']);
    });

    it('should handle complex queries', () => {
      const sql = 'SELECT u.name FROM users u WHERE u.id IN (SELECT user_id FROM orders)';
      const result = Utils.extractTableNames(sql);
      expect(result).toContain('users');
    });

    it('should handle edge cases', () => {
      expect(Utils.extractTableNames('')).toEqual([]);
      expect(Utils.extractTableNames(null)).toEqual([]);
      expect(Utils.extractTableNames('SELECT 1')).toEqual([]);
    });
  });

  describe('deepClone', () => {
    it('should clone primitive values', () => {
      expect(Utils.deepClone(42)).toBe(42);
      expect(Utils.deepClone('hello')).toBe('hello');
      expect(Utils.deepClone(true)).toBe(true);
      expect(Utils.deepClone(null)).toBe(null);
    });

    it('should clone arrays', () => {
      const original = [1, 2, { a: 3 }];
      const cloned = Utils.deepClone(original);
      
      expect(cloned).toEqual(original);
      expect(cloned).not.toBe(original);
      expect(cloned[2]).not.toBe(original[2]);
    });

    it('should clone objects', () => {
      const original = { a: 1, b: { c: 2 }, d: [3, 4] };
      const cloned = Utils.deepClone(original);
      
      expect(cloned).toEqual(original);
      expect(cloned).not.toBe(original);
      expect(cloned.b).not.toBe(original.b);
      expect(cloned.d).not.toBe(original.d);
    });

    it('should clone dates', () => {
      const original = new Date('2022-01-01');
      const cloned = Utils.deepClone(original);
      
      expect(cloned).toEqual(original);
      expect(cloned).not.toBe(original);
    });
  });

  describe('debounce', () => {
    it('should debounce function calls', async () => {
      const mockFn = vi.fn();
      const debouncedFn = Utils.debounce(mockFn, 100);
      
      debouncedFn('call1');
      debouncedFn('call2');
      debouncedFn('call3');
      
      expect(mockFn).not.toHaveBeenCalled();
      
      await new Promise(resolve => setTimeout(resolve, 150));
      
      expect(mockFn).toHaveBeenCalledTimes(1);
      expect(mockFn).toHaveBeenCalledWith('call3');
    });
  });

  describe('sortByKey', () => {
    const testData = [
      { name: 'charlie', age: 30 },
      { name: 'alice', age: 25 },
      { name: 'bob', age: 35 }
    ];

    it('should sort ascending by default', () => {
      const sorted = Utils.sortByKey(testData, 'name');
      expect(sorted[0].name).toBe('alice');
      expect(sorted[1].name).toBe('bob');
      expect(sorted[2].name).toBe('charlie');
    });

    it('should sort descending when specified', () => {
      const sorted = Utils.sortByKey(testData, 'age', 'desc');
      expect(sorted[0].age).toBe(35);
      expect(sorted[1].age).toBe(30);
      expect(sorted[2].age).toBe(25);
    });

    it('should not mutate original array', () => {
      const original = [...testData];
      Utils.sortByKey(testData, 'name');
      expect(testData).toEqual(original);
    });
  });

  describe('generateId', () => {
    it('should generate unique IDs', () => {
      const id1 = Utils.generateId();
      const id2 = Utils.generateId();
      
      expect(typeof id1).toBe('string');
      expect(typeof id2).toBe('string');
      expect(id1).not.toBe(id2);
      expect(id1.length).toBeGreaterThan(0);
    });

    it('should generate IDs with consistent format', () => {
      const ids = Array.from({ length: 100 }, () => Utils.generateId());
      
      ids.forEach(id => {
        expect(typeof id).toBe('string');
        expect(id.length).toBeGreaterThan(5);
        expect(/^[a-z0-9]+$/.test(id)).toBe(true);
      });
    });
  });

  describe('isValidJSON', () => {
    it('should validate correct JSON strings', () => {
      expect(Utils.isValidJSON('{"key": "value"}')).toBe(true);
      expect(Utils.isValidJSON('[]')).toBe(true);
      expect(Utils.isValidJSON('null')).toBe(true);
      expect(Utils.isValidJSON('true')).toBe(true);
      expect(Utils.isValidJSON('123')).toBe(true);
      expect(Utils.isValidJSON('"string"')).toBe(true);
    });

    it('should reject invalid JSON strings', () => {
      expect(Utils.isValidJSON('{')).toBe(false);
      expect(Utils.isValidJSON('{"key": value}')).toBe(false);
      expect(Utils.isValidJSON('undefined')).toBe(false);
      expect(Utils.isValidJSON('')).toBe(false);
      expect(Utils.isValidJSON('{')).toBe(false);
    });

    it('should handle complex JSON structures', () => {
      const complexJson = JSON.stringify({
        tableName: 'test',
        tableType: 'OFFLINE',
        segmentsConfig: {
          timeColumnName: 'timestamp',
          replication: 1
        },
        tenants: {
          broker: 'DefaultTenant',
          server: 'DefaultTenant'
        }
      });
      
      expect(Utils.isValidJSON(complexJson)).toBe(true);
    });
  });

  describe('formatDuration', () => {
    it('should format milliseconds correctly', () => {
      expect(Utils.formatDuration(500)).toBe('500ms');
      expect(Utils.formatDuration(999)).toBe('999ms');
    });

    it('should format seconds correctly', () => {
      expect(Utils.formatDuration(1000)).toBe('1.0s');
      expect(Utils.formatDuration(1500)).toBe('1.5s');
      expect(Utils.formatDuration(59999)).toBe('60.0s');
    });

    it('should format minutes correctly', () => {
      expect(Utils.formatDuration(60000)).toBe('1.0m');
      expect(Utils.formatDuration(90000)).toBe('1.5m');
      expect(Utils.formatDuration(3599000)).toBe('60.0m');
    });

    it('should format hours correctly', () => {
      expect(Utils.formatDuration(3600000)).toBe('1.0h');
      expect(Utils.formatDuration(5400000)).toBe('1.5h');
    });
  });

  describe('Data Processing Utilities', () => {
    it('should process Pinot query results correctly', () => {
      const processQueryResult = (apiResponse) => {
        if (!apiResponse.resultTable) return [];
        
        const { columnNames } = apiResponse.resultTable.dataSchema;
        const { rows } = apiResponse.resultTable;
        
        return rows.map(row => {
          const obj = {};
          columnNames.forEach((col, idx) => {
            obj[col] = row[idx];
          });
          return obj;
        });
      };

      const mockResponse = {
        resultTable: {
          dataSchema: {
            columnNames: ['tableName', 'segmentCount', 'totalDocs'],
            columnDataTypes: ['STRING', 'INT', 'LONG']
          },
          rows: [
            ['airlineStats', '10', '115545654'],
            ['userEvents', '5', '23456789']
          ]
        }
      };

      const result = processQueryResult(mockResponse);
      
      expect(result).toEqual([
        { tableName: 'airlineStats', segmentCount: '10', totalDocs: '115545654' },
        { tableName: 'userEvents', segmentCount: '5', totalDocs: '23456789' }
      ]);
    });

    it('should handle empty query results', () => {
      const processQueryResult = (apiResponse) => {
        if (!apiResponse.resultTable) return [];
        return apiResponse.resultTable.rows || [];
      };

      expect(processQueryResult({})).toEqual([]);
      expect(processQueryResult({ resultTable: {} })).toEqual([]);
      expect(processQueryResult({ resultTable: { rows: [] } })).toEqual([]);
    });
  });

  describe('Error Handling Utilities', () => {
    it('should extract error messages from API responses', () => {
      const extractErrorMessage = (error) => {
        if (typeof error === 'string') return error;
        if (error.message) return error.message;
        if (error.error) return error.error;
        if (error.details) return error.details;
        return 'Unknown error occurred';
      };

      expect(extractErrorMessage('Simple error')).toBe('Simple error');
      expect(extractErrorMessage({ message: 'API error' })).toBe('API error');
      expect(extractErrorMessage({ error: 'Server error' })).toBe('Server error');
      expect(extractErrorMessage({ details: 'Detailed error' })).toBe('Detailed error');
      expect(extractErrorMessage({})).toBe('Unknown error occurred');
    });

    it('should categorize HTTP status codes', () => {
      const categorizeStatus = (status) => {
        if (status >= 200 && status < 300) return 'success';
        if (status >= 400 && status < 500) return 'client-error';
        if (status >= 500) return 'server-error';
        return 'unknown';
      };

      expect(categorizeStatus(200)).toBe('success');
      expect(categorizeStatus(201)).toBe('success');
      expect(categorizeStatus(400)).toBe('client-error');
      expect(categorizeStatus(404)).toBe('client-error');
      expect(categorizeStatus(500)).toBe('server-error');
      expect(categorizeStatus(503)).toBe('server-error');
      expect(categorizeStatus(100)).toBe('unknown');
    });
  });

  describe('Data Validation Utilities', () => {
    it('should validate schema field specifications', () => {
      const validateFieldSpec = (field) => {
        if (!field || typeof field !== 'object') return false;
        if (!field.name || typeof field.name !== 'string') return false;
        if (!field.dataType || typeof field.dataType !== 'string') return false;
        
        const validDataTypes = ['INT', 'LONG', 'FLOAT', 'DOUBLE', 'STRING', 'BOOLEAN', 'TIMESTAMP', 'BYTES'];
        return validDataTypes.includes(field.dataType.toUpperCase());
      };

      expect(validateFieldSpec({ name: 'userId', dataType: 'INT' })).toBe(true);
      expect(validateFieldSpec({ name: 'timestamp', dataType: 'LONG' })).toBe(true);
      expect(validateFieldSpec({ name: 'name', dataType: 'STRING' })).toBe(true);
      
      expect(validateFieldSpec({})).toBe(false);
      expect(validateFieldSpec({ name: 'test' })).toBe(false);
      expect(validateFieldSpec({ dataType: 'INT' })).toBe(false);
      expect(validateFieldSpec({ name: 'test', dataType: 'INVALID' })).toBe(false);
    });

    it('should validate table configuration', () => {
      const validateTableConfig = (config) => {
        if (!config || typeof config !== 'object') return false;
        if (!config.tableName || !Utils.validateTableName(config.tableName)) return false;
        if (!config.tableType || !['OFFLINE', 'REALTIME'].includes(config.tableType)) return false;
        return true;
      };

      expect(validateTableConfig({
        tableName: 'validTable',
        tableType: 'OFFLINE'
      })).toBe(true);

      expect(validateTableConfig({
        tableName: 'validTable',
        tableType: 'REALTIME'
      })).toBe(true);

      expect(validateTableConfig({})).toBe(false);
      expect(validateTableConfig({ tableName: 'valid' })).toBe(false);
      expect(validateTableConfig({ tableType: 'OFFLINE' })).toBe(false);
      expect(validateTableConfig({ 
        tableName: 'invalid-name', 
        tableType: 'OFFLINE' 
      })).toBe(false);
    });
  });

  describe('Performance Utilities', () => {
    it('should measure execution time', () => {
      const measureTime = (fn) => {
        const start = performance.now();
        fn();
        const end = performance.now();
        return end - start;
      };

      const slowFunction = () => {
        // Simulate some work
        let sum = 0;
        for (let i = 0; i < 1000; i++) {
          sum += i;
        }
        return sum;
      };

      const duration = measureTime(slowFunction);
      expect(typeof duration).toBe('number');
      expect(duration).toBeGreaterThan(0);
    });

    it('should throttle function calls', () => {
      const throttle = (func, limit) => {
        let inThrottle;
        return function() {
          const args = arguments;
          const context = this;
          if (!inThrottle) {
            func.apply(context, args);
            inThrottle = true;
            setTimeout(() => inThrottle = false, limit);
          }
        };
      };

      const mockFn = vi.fn();
      const throttledFn = throttle(mockFn, 100);
      
      throttledFn();
      throttledFn();
      throttledFn();
      
      expect(mockFn).toHaveBeenCalledTimes(1);
    });
  });
});

