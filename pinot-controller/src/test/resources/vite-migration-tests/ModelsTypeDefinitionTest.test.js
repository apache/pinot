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
 * Test suite for Models.ts module and type definitions
 * Tests the migration from ambient module declarations to explicit TypeScript module
 */

const { describe, it, expect, beforeAll } = require('@jest/globals');
const fs = require('fs');
const path = require('path');

describe('Models.ts Type Definition Tests', () => {
  const modelsPath = path.join(__dirname, '../../../main/resources/app/Models.ts');
  let modelsContent;

  beforeAll(() => {
    if (fs.existsSync(modelsPath)) {
      modelsContent = fs.readFileSync(modelsPath, 'utf8');
    }
  });

  describe('Module Structure', () => {
    it('should exist as a real TypeScript module', () => {
      expect(fs.existsSync(modelsPath)).toBe(true);
      expect(modelsContent).toContain('export enum');
      expect(modelsContent).toContain('export type');
      expect(modelsContent).toContain('export interface');
    });

    it('should have proper Apache license header', () => {
      expect(modelsContent).toContain('Licensed to the Apache Software Foundation');
      expect(modelsContent).toContain('Apache License, Version 2.0');
    });

    it('should contain migration documentation', () => {
      expect(modelsContent).toContain('Central type/enum module for Pinot Controller UI');
      expect(modelsContent).toContain('Previously declared as an ambient module');
      expect(modelsContent).toContain('Vite uses an explicit alias');
    });
  });

  describe('Enum Definitions', () => {
    it('should convert const enum to regular enum', () => {
      expect(modelsContent).toContain('export enum AuthWorkflow');
      expect(modelsContent).toContain('export enum SEGMENT_STATUS');
      expect(modelsContent).toContain('export enum DISPLAY_SEGMENT_STATUS');
      expect(modelsContent).toContain('export enum InstanceState');
      expect(modelsContent).toContain('export enum InstanceType');
      expect(modelsContent).toContain('export enum TableType');
      expect(modelsContent).toContain('export enum TaskType');
      
      // Should not contain const enum declarations
      expect(modelsContent).not.toContain('const enum');
    });

    it('should have AuthWorkflow enum with correct values', () => {
      expect(modelsContent).toContain('NONE = \'NONE\'');
      expect(modelsContent).toContain('BASIC = \'BASIC\'');
      expect(modelsContent).toContain('OIDC = \'OIDC\'');
    });

    it('should have SEGMENT_STATUS enum with correct values', () => {
      expect(modelsContent).toContain('ONLINE = \'ONLINE\'');
      expect(modelsContent).toContain('OFFLINE = \'OFFLINE\'');
      expect(modelsContent).toContain('CONSUMING = \'CONSUMING\'');
      expect(modelsContent).toContain('ERROR = \'ERROR\'');
    });

    it('should have InstanceType enum with correct values', () => {
      expect(modelsContent).toContain('BROKER = \'BROKER\'');
      expect(modelsContent).toContain('CONTROLLER = \'CONTROLLER\'');
      expect(modelsContent).toContain('MINION = \'MINION\'');
      expect(modelsContent).toContain('SERVER = \'SERVER\'');
    });

    it('should have TableType enum with correct values', () => {
      expect(modelsContent).toContain('REALTIME = \'realtime\'');
      expect(modelsContent).toContain('OFFLINE = \'offline\'');
    });
  });

  describe('Type Definitions', () => {
    it('should export SegmentStatus type', () => {
      expect(modelsContent).toContain('export type SegmentStatus');
      expect(modelsContent).toContain('value: DISPLAY_SEGMENT_STATUS');
      expect(modelsContent).toContain('tooltip: string');
      expect(modelsContent).toContain('component?: JSX.Element');
    });

    it('should export TableData type', () => {
      expect(modelsContent).toContain('export type TableData');
      expect(modelsContent).toContain('records: Array');
      expect(modelsContent).toContain('columns: Array<string>');
      expect(modelsContent).toContain('error?: string');
      expect(modelsContent).toContain('isLoading?: boolean');
    });

    it('should export PinotTableDetails type', () => {
      expect(modelsContent).toContain('export type PinotTableDetails');
      expect(modelsContent).toContain('name: string');
      expect(modelsContent).toContain('reported_size: string');
      expect(modelsContent).toContain('estimated_size: string');
      expect(modelsContent).toContain('number_of_segments: string');
      expect(modelsContent).toContain('segment_status: SegmentStatus');
    });

    it('should export Instance type', () => {
      expect(modelsContent).toContain('export type Instance');
      expect(modelsContent).toContain('instanceName: string');
      expect(modelsContent).toContain('hostName: string');
      expect(modelsContent).toContain('enabled: boolean');
      expect(modelsContent).toContain('port: number');
      expect(modelsContent).toContain('grpcPort: number');
    });

    it('should export SQLResult type', () => {
      expect(modelsContent).toContain('export type SQLResult');
      expect(modelsContent).toContain('resultTable:');
      expect(modelsContent).toContain('timeUsedMs: number');
      expect(modelsContent).toContain('numDocsScanned: number');
      expect(modelsContent).toContain('totalDocs: number');
    });
  });

  describe('Interface Definitions', () => {
    it('should export ConsumingInfo interface', () => {
      expect(modelsContent).toContain('export interface ConsumingInfo');
      expect(modelsContent).toContain('serverName: string');
      expect(modelsContent).toContain('consumerState: string');
      expect(modelsContent).toContain('lastConsumedTimestamp: number');
      expect(modelsContent).toContain('partitionToOffsetMap: Record<string, string>');
    });

    it('should export ConsumingSegmentsInfo interface', () => {
      expect(modelsContent).toContain('export interface ConsumingSegmentsInfo');
      expect(modelsContent).toContain('serversFailingToRespond: number');
      expect(modelsContent).toContain('serversUnparsableRespond: number');
      expect(modelsContent).toContain('_segmentToConsumingInfoMap: Record<string, ConsumingInfo[]>');
    });

    it('should export PauseStatusDetails interface', () => {
      expect(modelsContent).toContain('export interface PauseStatusDetails');
      expect(modelsContent).toContain('pauseFlag: boolean');
      expect(modelsContent).toContain('consumingSegments: string[]');
      expect(modelsContent).toContain('reasonCode: string');
      expect(modelsContent).toContain('comment: string');
      expect(modelsContent).toContain('timestamp: string');
    });

    it('should export TaskProgressResponse interface', () => {
      expect(modelsContent).toContain('export interface TaskProgressResponse');
      expect(modelsContent).toContain('[key: string]: TaskProgressStatus[] | string');
    });

    it('should export TaskRuntimeConfig interface', () => {
      expect(modelsContent).toContain('export interface TaskRuntimeConfig');
      expect(modelsContent).toContain('ConcurrentTasksPerWorker: string');
      expect(modelsContent).toContain('TaskTimeoutMs: string');
      expect(modelsContent).toContain('TaskExpireTimeMs: string');
      expect(modelsContent).toContain('MinionWorkerGroupTag: string');
    });

    it('should export SegmentDebugDetails interface', () => {
      expect(modelsContent).toContain('export interface SegmentDebugDetails');
      expect(modelsContent).toContain('segmentName: string');
      expect(modelsContent).toContain('serverState:');
    });

    it('should export SqlException interface', () => {
      expect(modelsContent).toContain('export interface SqlException');
      expect(modelsContent).toContain('errorCode: number');
      expect(modelsContent).toContain('message: string');
    });
  });

  describe('esbuild Compatibility', () => {
    it('should not use const enum due to esbuild limitations', () => {
      expect(modelsContent).not.toContain('const enum');
      expect(modelsContent).toContain('esbuild (used by Vite) does not support cross-file const enum');
    });

    it('should have runtime enum values', () => {
      // Regular enums have runtime values, const enums are inlined
      expect(modelsContent).toContain('Runtime enums');
      expect(modelsContent).toContain('values now live at runtime, not inlined');
    });
  });

});

describe('Models.ts Module Resolution Tests', () => {
  const viteConfigPath = path.join(__dirname, '../../../main/resources/vite.config.ts');
  
  it('should be properly aliased in Vite configuration', () => {
    if (fs.existsSync(viteConfigPath)) {
      const viteConfig = fs.readFileSync(viteConfigPath, 'utf8');
      expect(viteConfig).toContain('Models: path.join(__dirname, \'app\', \'Models.ts\')');
    }
  });

  it('should replace webpack resolve.modules configuration', () => {
    // Ensure no webpack configuration remains
    const resourcesDir = path.join(__dirname, '../../../main/resources');
    const webpackConfigPath = path.join(resourcesDir, 'webpack.config.js');
    
    if (fs.existsSync(webpackConfigPath)) {
      const webpackConfig = fs.readFileSync(webpackConfigPath, 'utf8');
      // If webpack config exists, it should not contain Models resolution
      expect(webpackConfig).not.toContain('resolve.modules');
    }
  });
});
