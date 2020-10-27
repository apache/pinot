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

import { AxiosResponse } from 'axios';
import { TableData, Instances, Instance, Tenants, ClusterConfig, TableName, TableSize,
  IdealState, QueryTables, TableSchema, SQLResult, ClusterName, ZKGetList, ZKConfig, OperationResponse,
  BrokerList, ServerList
} from 'Models';

const headers = {
  'Content-Type': 'application/json; charset=UTF-8',
  'Accept': 'text/plain, */*; q=0.01'
};

import { baseApi } from '../utils/axios-config';

export const getTenants = (): Promise<AxiosResponse<Tenants>> =>
  baseApi.get('/tenants');

export const getTenant = (name: string): Promise<AxiosResponse<TableData>> =>
  baseApi.get(`/tenants/${name}`);

export const getTenantTable = (name: string): Promise<AxiosResponse<TableName>> =>
  baseApi.get(`/tenants/${name}/tables`);

export const getTenantTableDetails = (tableName: string): Promise<AxiosResponse<IdealState>> =>
  baseApi.get(`/tables/${tableName}`);

export const putTable = (name: string, params: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.put(`/tables/${name}`, params, { headers });

export const putSchema = (name: string, params: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.put(`/schemas/${name}`, params, { headers });

export const getSegmentMetadata = (tableName: string, segmentName: string): Promise<AxiosResponse<IdealState>> =>
  baseApi.get(`/segments/${tableName}/${segmentName}/metadata`);

export const getTableSize = (name: string): Promise<AxiosResponse<TableSize>> =>
  baseApi.get(`/tables/${name}/size`);

export const getIdealState = (name: string): Promise<AxiosResponse<IdealState>> =>
  baseApi.get(`/tables/${name}/idealstate`);

export const getExternalView = (name: string): Promise<AxiosResponse<IdealState>> =>
  baseApi.get(`/tables/${name}/externalview`);

export const getInstances = (): Promise<AxiosResponse<Instances>> =>
  baseApi.get('/instances');

export const getInstance = (name: string): Promise<AxiosResponse<Instance>> =>
  baseApi.get(`/instances/${name}`);

export const putInstance = (name: string, params: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.put(`/instances/${name}`, params, { headers });

export const updateInstanceTags = (name: string, params: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.put(`/instances/${name}/updateTags?tags=${params}`, null, { headers });

export const setInstanceState = (name: string, stateName: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.post(`/instances/${name}/state`, stateName, { headers: {'Content-Type': 'text/plain', 'Accept': 'application/json'} });

export const setTableState = (name: string, stateName: string, tableType: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.get(`/tables/${name}?state=${stateName}&type=${tableType}`);

export const dropInstance = (name: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.delete(`instances/${name}`, { headers });

export const getClusterConfig = (): Promise<AxiosResponse<ClusterConfig>> =>
  baseApi.get('/cluster/configs');

export const getQueryTables = (type?: string): Promise<AxiosResponse<QueryTables>> =>
  baseApi.get(`/tables${type ? `?type=${type}`: ''}`);

export const getTableSchema = (name: string): Promise<AxiosResponse<TableSchema>> =>
  baseApi.get(`/tables/${name}/schema`);

export const getQueryResult = (params: Object, url: string): Promise<AxiosResponse<SQLResult>> =>
  baseApi.post(`/${url}`, params, {headers});

export const getClusterInfo = (): Promise<AxiosResponse<ClusterName>> =>
  baseApi.get('/cluster/info');

export const zookeeperGetList = (params: string): Promise<AxiosResponse<ZKGetList>> =>
  baseApi.get(`/zk/ls?path=${params}`);

export const zookeeperGetData = (params: string): Promise<AxiosResponse<ZKConfig>> =>
  baseApi.get(`/zk/get?path=${params}`);

export const zookeeperGetStat = (params: string): Promise<AxiosResponse<ZKConfig>> =>
  baseApi.get(`/zk/stat?path=${params}`);

export const zookeeperGetListWithStat = (params: string): Promise<AxiosResponse<ZKConfig>> =>
  baseApi.get(`/zk/lsl?path=${params}`);

export const zookeeperPutData = (params: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.put(`/zk/put?${params}`, null, { headers });

export const zookeeperDeleteNode = (params: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.delete(`/zk/delete?path=${params}`);

export const getBrokerListOfTenant = (name: string): Promise<AxiosResponse<BrokerList>> =>
  baseApi.get(`/brokers/tenants/${name}`);

export const getServerListOfTenant = (name: string): Promise<AxiosResponse<ServerList>> =>
  baseApi.get(`/tenants/${name}?type=server`);

export const reloadSegment = (tableName: string, instanceName: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.post(`/segments/${tableName}/${instanceName}/reload`, null, {headers});

export const reloadAllSegments = (tableName: string, tableType: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.post(`/segments/${tableName}/reload?type=${tableType}`, null, {headers});

export const reloadStatus = (tableName: string, tableType: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.get(`/segments/${tableName}/metadata?type=${tableType}`);

export const deleteSegment = (tableName: string, instanceName: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.delete(`/segments/${tableName}/${instanceName}`, {headers});

export const deleteTable = (tableName: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.delete(`/tables/${tableName}`, {headers});

export const deleteSchema = (schemaName: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.delete(`/schemas/${schemaName}`, {headers});

export const rebalanceServersForTable = (tableName: string, qParams: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.post(`/tables/${tableName}/rebalance?${qParams}`, null, {headers});

export const rebalanceBrokersForTable = (tableName: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.post(`/tables/${tableName}/rebuildBrokerResourceFromHelixTags`, null, {headers});