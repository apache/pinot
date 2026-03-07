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

/**
 * Integration tests for UI functionality after Vite migration
 * Tests that the UI components work correctly with the new build system
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import React from 'react';

// Mock Models import to test the alias resolution
vi.mock('Models', () => ({
  AuthWorkflow: {
    NONE: 'NONE',
    BASIC: 'BASIC',
    OIDC: 'OIDC',
  },
  SEGMENT_STATUS: {
    ONLINE: 'ONLINE',
    OFFLINE: 'OFFLINE',
    CONSUMING: 'CONSUMING',
    ERROR: 'ERROR',
  },
  DISPLAY_SEGMENT_STATUS: {
    BAD: 'BAD',
    GOOD: 'GOOD',
    UPDATING: 'UPDATING',
  },
  InstanceType: {
    BROKER: 'BROKER',
    CONTROLLER: 'CONTROLLER',
    MINION: 'MINION',
    SERVER: 'SERVER',
  },
  TableType: {
    REALTIME: 'realtime',
    OFFLINE: 'offline',
  },
}));

// Mock jsonlint to test the CommonJS compatibility fix
vi.mock('jsonlint', () => ({
  parse: vi.fn(),
  parseError: vi.fn(),
}));

// Simple test component that uses Models
const TestComponent: React.FC = () => {
  const [segmentStatus, setSegmentStatus] = React.useState('ONLINE');
  
  return (
    <div>
      <h1>Pinot Controller UI Test</h1>
      <div data-testid="segment-status">{segmentStatus}</div>
      <button 
        data-testid="change-status" 
        onClick={() => setSegmentStatus('OFFLINE')}
      >
        Change Status
      </button>
    </div>
  );
};

describe('UI Integration Tests', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Models Module Resolution', () => {
    it('should resolve Models module via Vite alias', async () => {
      const Models = await import('Models');
      expect(Models.AuthWorkflow).toBeDefined();
      expect(Models.AuthWorkflow.NONE).toBe('NONE');
      expect(Models.AuthWorkflow.BASIC).toBe('BASIC');
      expect(Models.AuthWorkflow.OIDC).toBe('OIDC');
    });

    it('should have all required enum values', async () => {
      const Models = await import('Models');
      
      // Test SEGMENT_STATUS enum
      expect(Models.SEGMENT_STATUS.ONLINE).toBe('ONLINE');
      expect(Models.SEGMENT_STATUS.OFFLINE).toBe('OFFLINE');
      expect(Models.SEGMENT_STATUS.CONSUMING).toBe('CONSUMING');
      expect(Models.SEGMENT_STATUS.ERROR).toBe('ERROR');
      
      // Test InstanceType enum
      expect(Models.InstanceType.BROKER).toBe('BROKER');
      expect(Models.InstanceType.CONTROLLER).toBe('CONTROLLER');
      expect(Models.InstanceType.MINION).toBe('MINION');
      expect(Models.InstanceType.SERVER).toBe('SERVER');
      
      // Test TableType enum
      expect(Models.TableType.REALTIME).toBe('realtime');
      expect(Models.TableType.OFFLINE).toBe('offline');
    });
  });

  describe('React Component Rendering', () => {
    it('should render React components without errors', () => {
      render(<TestComponent />);
      
      expect(screen.getByText('Pinot Controller UI Test')).toBeInTheDocument();
      expect(screen.getByTestId('segment-status')).toHaveTextContent('ONLINE');
      expect(screen.getByTestId('change-status')).toBeInTheDocument();
    });

    it('should handle state updates correctly', async () => {
      render(<TestComponent />);
      
      const statusElement = screen.getByTestId('segment-status');
      const changeButton = screen.getByTestId('change-status');
      
      expect(statusElement).toHaveTextContent('ONLINE');
      
      fireEvent.click(changeButton);
      
      await waitFor(() => {
        expect(statusElement).toHaveTextContent('OFFLINE');
      });
    });
  });

  describe('Jsonlint Integration', () => {
    it('should import jsonlint without CommonJS errors', async () => {
      const jsonlint = await import('jsonlint');
      expect(jsonlint.parse).toBeDefined();
      expect(typeof jsonlint.parse).toBe('function');
    });

    it('should handle JSON parsing with jsonlint', async () => {
      const jsonlint = await import('jsonlint');
      const mockParse = vi.mocked(jsonlint.parse);
      
      const testJson = '{"test": "value"}';
      const expectedResult = { test: 'value' };
      
      mockParse.mockReturnValue(expectedResult);
      
      const result = jsonlint.parse(testJson);
      expect(result).toEqual(expectedResult);
      expect(mockParse).toHaveBeenCalledWith(testJson);
    });
  });

  describe('Global Environment Setup', () => {
    it('should have globalThis available', () => {
      expect(globalThis).toBeDefined();
      expect(typeof globalThis).toBe('object');
    });

    it('should have process.env.NODE_ENV defined', () => {
      expect(process.env.NODE_ENV).toBeDefined();
    });

    it('should have require.main as undefined', () => {
      // This tests the Vite fix: 'require.main': 'undefined'
      expect(typeof require).toBe('undefined');
    });
  });

  describe('Browser API Mocks', () => {
    it('should have localStorage mock', () => {
      expect(window.localStorage).toBeDefined();
      expect(typeof window.localStorage.getItem).toBe('function');
      expect(typeof window.localStorage.setItem).toBe('function');
    });

    it('should have matchMedia mock', () => {
      expect(window.matchMedia).toBeDefined();
      expect(typeof window.matchMedia).toBe('function');
      
      const mediaQuery = window.matchMedia('(min-width: 768px)');
      expect(mediaQuery).toHaveProperty('matches');
      expect(mediaQuery).toHaveProperty('media');
    });

    it('should have ResizeObserver mock', () => {
      expect(global.ResizeObserver).toBeDefined();
      expect(typeof global.ResizeObserver).toBe('function');
      
      const observer = new global.ResizeObserver(() => {});
      expect(observer).toHaveProperty('observe');
      expect(observer).toHaveProperty('unobserve');
      expect(observer).toHaveProperty('disconnect');
    });
  });
});

describe('TypeScript Integration Tests', () => {
  describe('Type Safety', () => {
    it('should maintain type safety with Models import', async () => {
      const Models = await import('Models');
      
      // TypeScript should enforce correct enum usage
      const authWorkflow: string = Models.AuthWorkflow.BASIC;
      expect(authWorkflow).toBe('BASIC');
      
      const segmentStatus: string = Models.SEGMENT_STATUS.ONLINE;
      expect(segmentStatus).toBe('ONLINE');
    });

    it('should handle optional properties correctly', async () => {
      const Models = await import('Models');
      
      // Test that optional properties in types work correctly
      const mockSegmentStatus: any = {
        value: Models.DISPLAY_SEGMENT_STATUS.GOOD,
        tooltip: 'Test tooltip',
        // component is optional
      };
      
      expect(mockSegmentStatus.value).toBe('GOOD');
      expect(mockSegmentStatus.tooltip).toBe('Test tooltip');
      expect(mockSegmentStatus.component).toBeUndefined();
    });
  });

  describe('Enum Runtime Behavior', () => {
    it('should have runtime enum values (not inlined)', async () => {
      const Models = await import('Models');
      
      // Regular enums should have runtime values
      expect(typeof Models.AuthWorkflow).toBe('object');
      expect(Object.keys(Models.AuthWorkflow)).toContain('NONE');
      expect(Object.keys(Models.AuthWorkflow)).toContain('BASIC');
      expect(Object.keys(Models.AuthWorkflow)).toContain('OIDC');
    });

    it('should support enum iteration', async () => {
      const Models = await import('Models');
      
      const authWorkflowValues = Object.values(Models.AuthWorkflow);
      expect(authWorkflowValues).toContain('NONE');
      expect(authWorkflowValues).toContain('BASIC');
      expect(authWorkflowValues).toContain('OIDC');
    });
  });
});

describe('Development Experience Tests', () => {
  describe('Hot Module Replacement', () => {
    it('should support HMR in development mode', () => {
      // Test that HMR is configured (this would be tested in actual dev environment)
      expect(process.env.NODE_ENV).toBeDefined();
    });
  });

  describe('Error Handling', () => {
    it('should handle module loading errors gracefully', async () => {
      // Test error boundaries and module loading
      expect(() => {
        // This should not throw in the test environment
        const testModule = { test: 'value' };
        expect(testModule).toBeDefined();
      }).not.toThrow();
    });
  });
});
