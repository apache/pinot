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

declare module 'Models' {
  export type TableData = {
    records: Array<Array<string | number | boolean>>;
    columns: Array<string>;
  };

  export type Tenants = {
    SERVER_TENANTS: Array<string>;
    BROKER_TENANTS: Array<string>;
  };

  export type Instances = {
    instances: Array<string>;
  };

  export type Instance = {
    instanceName: string;
    hostName: string;
    enabled: boolean;
    port: number;
    tags: Array<string>;
    pools?: string;
  };

  export type ClusterConfig = {
    [name: string]: string;
  };

  export type TableName = {
    tables: Array<string>;
  };

  export type TableSize = {
    tableName: string;
    reportedSizeInBytes: number;
    estimatedSizeInBytes: number;
    offlineSegments: Segments | null;
    realtimeSegments: Segments | null;
  };

  type Segments = {
    reportedSizeInBytes: number;
    estimatedSizeInBytes: number;
    missingSegments: number;
    segments: Object;
  };

  export type IdealState = {
    OFFLINE: Object | null;
    REALTIME: Object | null;
  };

  export type QueryTables = {
    tables: Array<string>;
  };

  export type TableSchema = {
    dimensionFieldSpecs: Array<schema>;
    metricFieldSpecs?: Array<schema>;
    dateTimeFieldSpecs?: Array<schema>;
  };

  type schema = {
    name: string,
    dataType: string
    fieldType?: string
  };

  export type SQLResult = {
    resultTable: {
      dataSchema: {
        columnDataTypes: Array<string>;
        columnNames: Array<string>;
      }
      rows: Array<Array<number | string>>;
    },
    timeUsedMs: number
    numDocsScanned: number
    totalDocs: number
    numServersQueried: number
    numServersResponded: number
    numSegmentsQueried: number
    numSegmentsProcessed: number
    numSegmentsMatched: number
    numConsumingSegmentsQueried: number
    numEntriesScannedInFilter: number
    numEntriesScannedPostFilter: number
    numGroupsLimitReached: boolean
    partialResponse?: number
    minConsumingFreshnessTimeMs: number
  };

  export type ClusterName = {
    clusterName: string
  };

  export type ZKGetList = Array<string>;

  export type ZKConfig = {
    ctime: any,
    mtime: any
  };
  export type OperationResponse = any;

  export type DataTable = {
    [name: string]: Array<string>
  };

  export type BrokerList = Array<string>;

  export type ServerList = {
    ServerInstances: Array<string>,
    tenantName: string
  }
}
