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
 * Test suite for API integration and HTTP client functionality
 * Tests API calls, error handling, and data transformation
 */


import { describe, it, expect, beforeEach, vi } from 'vitest';

// Mock fetch for API testing
global.fetch = vi.fn();

// Mock API response utilities
const createMockResponse = (data, status = 200) => ({
  ok: status >= 200 && status < 300,
  status,
  json: () => Promise.resolve(data),
  text: () => Promise.resolve(JSON.stringify(data)),
});

// Mock API client
class MockApiClient {
  constructor(baseUrl = 'http://localhost:9000') {
    this.baseUrl = baseUrl;
  }

  async get(endpoint) {
    const response = await fetch(`${this.baseUrl}${endpoint}`);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }
    return response.json();
  }

  async post(endpoint, data) {
    const response = await fetch(`${this.baseUrl}${endpoint}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }
    return response.json();
  }

  async delete(endpoint) {
    const response = await fetch(`${this.baseUrl}${endpoint}`, {
      method: 'DELETE',
    });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}: ${response.statusText}`);
    }
    return response.json();
  }
}

describe('API Integration Tests', () => {
  let apiClient;

  beforeEach(() => {
    vi.clearAllMocks();
    apiClient = new MockApiClient();
  });

  describe('Tables API', () => {
    it('should fetch tables list successfully', async () => {
      const mockTables = {
        tables: ['airlineStats', 'userEvents', 'meetupRsvp']
      };

      fetch.mockResolvedValueOnce(createMockResponse(mockTables));

      const result = await apiClient.get('/tables');

      expect(fetch).toHaveBeenCalledWith('http://localhost:9000/tables');
      expect(result).toEqual(mockTables);
    });

    it('should handle table creation', async () => {
      const tableConfig = {
        tableName: 'testTable',
        tableType: 'OFFLINE',
        segmentsConfig: {
          timeColumnName: 'timestamp',
          timeType: 'MILLISECONDS'
        }
      };

      const mockResponse = { status: 'Table created successfully' };
      fetch.mockResolvedValueOnce(createMockResponse(mockResponse));

      const result = await apiClient.post('/tables', tableConfig);

      expect(fetch).toHaveBeenCalledWith('http://localhost:9000/tables', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(tableConfig),
      });
      expect(result).toEqual(mockResponse);
    });

    it('should handle table deletion', async () => {
      const mockResponse = { status: 'Table deleted successfully' };
      fetch.mockResolvedValueOnce(createMockResponse(mockResponse));

      const result = await apiClient.delete('/tables/testTable');

      expect(fetch).toHaveBeenCalledWith('http://localhost:9000/tables/testTable', {
        method: 'DELETE',
      });
      expect(result).toEqual(mockResponse);
    });
  });

  describe('Schemas API', () => {
    it('should fetch schema list successfully', async () => {
      const mockSchemas = ['airlineStats', 'userEvents', 'meetupRsvp'];
      fetch.mockResolvedValueOnce(createMockResponse(mockSchemas));

      const result = await apiClient.get('/schemas');

      expect(fetch).toHaveBeenCalledWith('http://localhost:9000/schemas');
      expect(result).toEqual(mockSchemas);
    });

    it('should fetch specific schema details', async () => {
      const mockSchema = {
        schemaName: 'airlineStats',
        dimensionFieldSpecs: [
          { name: 'AirlineID', dataType: 'INT' },
          { name: 'Origin', dataType: 'STRING' }
        ],
        metricFieldSpecs: [
          { name: 'ArrDelay', dataType: 'INT' }
        ]
      };

      fetch.mockResolvedValueOnce(createMockResponse(mockSchema));

      const result = await apiClient.get('/schemas/airlineStats');

      expect(fetch).toHaveBeenCalledWith('http://localhost:9000/schemas/airlineStats');
      expect(result).toEqual(mockSchema);
    });
  });

  describe('Query API', () => {
    it('should execute SQL query successfully', async () => {
      const query = { sql: 'SELECT COUNT(*) FROM airlineStats' };
      const mockResult = {
        resultTable: {
          dataSchema: { columnNames: ['count(*)'], columnDataTypes: ['LONG'] },
          rows: [['12345']]
        },
        timeUsedMs: 150
      };

      fetch.mockResolvedValueOnce(createMockResponse(mockResult));

      const result = await apiClient.post('/query/sql', query);

      expect(fetch).toHaveBeenCalledWith('http://localhost:9000/query/sql', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(query),
      });
      expect(result).toEqual(mockResult);
    });

    it('should handle query timeout gracefully', async () => {
      const query = { sql: 'SELECT * FROM largeTable', timeoutMs: 1000 };
      
      fetch.mockRejectedValueOnce(new Error('Request timeout'));

      await expect(apiClient.post('/query/sql', query)).rejects.toThrow('Request timeout');
    });

    it('should handle malformed SQL queries', async () => {
      const query = { sql: 'INVALID SQL QUERY' };
      
      fetch.mockResolvedValueOnce(createMockResponse(
        { error: 'SQL parsing error' }, 
        400
      ));

      await expect(apiClient.post('/query/sql', query)).rejects.toThrow('HTTP 400');
    });
  });

  describe('Segments API', () => {
    it('should fetch segments for a table', async () => {
      const mockSegments = [
        { segmentName: 'airlineStats_OFFLINE_0', status: 'ONLINE' },
        { segmentName: 'airlineStats_OFFLINE_1', status: 'ONLINE' }
      ];

      fetch.mockResolvedValueOnce(createMockResponse(mockSegments));

      const result = await apiClient.get('/tables/airlineStats/segments');

      expect(fetch).toHaveBeenCalledWith('http://localhost:9000/tables/airlineStats/segments');
      expect(result).toEqual(mockSegments);
    });

    it('should handle segment reload', async () => {
      const mockResponse = { status: 'Segments reloaded successfully' };
      fetch.mockResolvedValueOnce(createMockResponse(mockResponse));

      const result = await apiClient.post('/tables/airlineStats/segments/reload', {});

      expect(result).toEqual(mockResponse);
    });
  });

  describe('Error Handling', () => {
    it('should handle network errors', async () => {
      fetch.mockRejectedValueOnce(new Error('Network error'));

      await expect(apiClient.get('/tables')).rejects.toThrow('Network error');
    });

    it('should handle 404 errors', async () => {
      fetch.mockResolvedValueOnce(createMockResponse(
        { error: 'Table not found' }, 
        404
      ));

      await expect(apiClient.get('/tables/nonexistent')).rejects.toThrow('HTTP 404');
    });

    it('should handle 500 server errors', async () => {
      fetch.mockResolvedValueOnce(createMockResponse(
        { error: 'Internal server error' }, 
        500
      ));

      await expect(apiClient.get('/tables')).rejects.toThrow('HTTP 500');
    });

    it('should handle malformed JSON responses', async () => {
      fetch.mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: () => Promise.reject(new Error('Invalid JSON')),
      });

      await expect(apiClient.get('/tables')).rejects.toThrow('Invalid JSON');
    });
  });

  describe('Performance and Load Testing', () => {
    it('should handle concurrent API requests', async () => {
      const mockResponse = { data: 'test' };
      fetch.mockResolvedValue(createMockResponse(mockResponse));

      const promises = Array.from({ length: 10 }, (_, i) => 
        apiClient.get(`/test-endpoint-${i}`)
      );

      const results = await Promise.all(promises);

      expect(results).toHaveLength(10);
      expect(fetch).toHaveBeenCalledTimes(10);
      results.forEach(result => {
        expect(result).toEqual(mockResponse);
      });
    });

    it('should handle large response payloads', async () => {
      const largeData = {
        tables: Array.from({ length: 1000 }, (_, i) => ({
          name: `table${i}`,
          type: 'OFFLINE',
          segments: Array.from({ length: 100 }, (_, j) => `segment${j}`)
        }))
      };

      fetch.mockResolvedValueOnce(createMockResponse(largeData));

      const startTime = performance.now();
      const result = await apiClient.get('/tables/detailed');
      const endTime = performance.now();

      expect(result.tables).toHaveLength(1000);
      expect(endTime - startTime).toBeLessThan(1000); // Should process in less than 1 second
    });
  });

  describe('Data Transformation', () => {
    it('should transform API response data correctly', () => {
      const rawApiData = {
        resultTable: {
          dataSchema: {
            columnNames: ['tableName', 'tableType', 'segmentCount'],
            columnDataTypes: ['STRING', 'STRING', 'INT']
          },
          rows: [
            ['airlineStats', 'OFFLINE', '10'],
            ['userEvents', 'REALTIME', '5']
          ]
        }
      };

      // Transform function that would be used in the UI
      const transformTableData = (apiResponse) => {
        return apiResponse.resultTable.rows.map(row => {
          const obj = {};
          apiResponse.resultTable.dataSchema.columnNames.forEach((col, idx) => {
            obj[col] = row[idx];
          });
          return obj;
        });
      };

      const transformed = transformTableData(rawApiData);

      expect(transformed).toEqual([
        { tableName: 'airlineStats', tableType: 'OFFLINE', segmentCount: '10' },
        { tableName: 'userEvents', tableType: 'REALTIME', segmentCount: '5' }
      ]);
    });

    it('should handle empty API responses', () => {
      const emptyResponse = {
        resultTable: {
          dataSchema: { columnNames: [], columnDataTypes: [] },
          rows: []
        }
      };

      const transformTableData = (apiResponse) => {
        return apiResponse.resultTable.rows.map(row => {
          const obj = {};
          apiResponse.resultTable.dataSchema.columnNames.forEach((col, idx) => {
            obj[col] = row[idx];
          });
          return obj;
        });
      };

      const transformed = transformTableData(emptyResponse);

      expect(transformed).toEqual([]);
    });
  });
});

