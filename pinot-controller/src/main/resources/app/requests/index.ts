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
  BrokerList, ServerList, UserList, TableList, UserObject, TaskProgressResponse, TableSegmentJobs, TaskRuntimeConfig,
  SegmentDebugDetails, QuerySchemas, TableType, InstanceState
} from 'Models';

const headers = {
  'Content-Type': 'application/json; charset=UTF-8',
  'Accept': 'text/plain, */*; q=0.01'
};

import { baseApi, baseApiWithErrors, transformApi } from '../utils/axios-config';

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

export const getSchemaList = (): Promise<AxiosResponse<QuerySchemas>> =>
  baseApi.get(`/schemas`);

export const getSchema = (name: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.get(`/schemas/${name}`);

export const putSchema = (name: string, params: string, reload?: boolean): Promise<AxiosResponse<OperationResponse>> => {
  let queryParams = {};
  
  if(reload) {
    queryParams["reload"] = reload;
  }

  return baseApi.put(`/schemas/${name}`, params, { headers, params: queryParams });
}

export const getSegmentMetadata = (tableName: string, segmentName: string): Promise<AxiosResponse<IdealState>> =>
  baseApi.get(`/segments/${tableName}/${segmentName}/metadata?columns=*`);

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

export const setInstanceState = (name: string, state: InstanceState): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.put(`/instances/${name}/state?state=${state}`, { headers: {'Content-Type': 'text/plain', 'Accept': 'application/json'} });

export const setTableState = (tableName: string, state: InstanceState, tableType: TableType): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.put(`/tables/${tableName}/state?state=${state}&type=${tableType}`);

export const dropInstance = (name: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.delete(`instances/${name}`, { headers });

export const getPeriodicTaskNames = (): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.get(`/periodictask/names`, { headers });

export const getTaskTypes = (): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.get(`/tasks/tasktypes`, { headers: { ...headers, Accept: 'application/json' } });

export const getTaskTypeTasks = (taskType: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.get(`/tasks/${taskType}/tasks`, { headers: { ...headers, Accept: 'application/json' } });

export const getTaskTypeState = (taskType: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.get(`/tasks/${taskType}/state`, { headers: { ...headers, Accept: 'application/json' } });

export const stopTasks = (taskType: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.put(`/tasks/${taskType}/stop`, { headers: { ...headers, Accept: 'application/json' } });

export const resumeTasks = (taskType: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.put(`/tasks/${taskType}/resume`, { headers: { ...headers, Accept: 'application/json' } });

export const cleanupTasks = (taskType: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.put(`/tasks/${taskType}/cleanup`, { headers: { ...headers, Accept: 'application/json' } });

export const deleteTasks = (taskType: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.delete(`/tasks/${taskType}`, { headers: { ...headers, Accept: 'application/json' } });

export const scheduleTask = (tableName: string, taskType: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.post(`/tasks/schedule?tableName=${tableName}&taskType=${taskType}`, null, { headers: { ...headers, Accept: 'application/json' } });

export const executeTask = (data): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.post(`/tasks/execute`, data, { headers: { ...headers, Accept: 'application/json' } });

export const getJobDetail = (tableName: string, taskType: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.get(`/tasks/scheduler/jobDetails?tableName=${tableName}&taskType=${taskType}`, { headers: { ...headers, Accept: 'application/json' } });
  
export const getMinionMeta = (tableName: string, taskType: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.get(`/tasks/${taskType}/${tableName}/metadata`, { headers: { ...headers, Accept: 'application/json' } });
  
export const getTasks = (tableName: string, taskType: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.get(`/tasks/${taskType}/${tableName}/state`, { headers: { ...headers, Accept: 'application/json' } });

export const getTaskRuntimeConfig = (taskName: string): Promise<AxiosResponse<TaskRuntimeConfig>> =>
  baseApi.get(`/tasks/task/${taskName}/runtime/config`, { headers: { ...headers, Accept: 'application/json' }});

export const getTaskDebug = (taskName: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.get(`/tasks/task/${taskName}/debug?verbosity=1`, { headers: { ...headers, Accept: 'application/json' } });

export const getTaskProgress = (taskName: string, subTaskName: string): Promise<AxiosResponse<TaskProgressResponse>> =>
  baseApi.get(`/tasks/subtask/${taskName}/progress`, { headers: { ...headers, Accept: 'application/json' }, params: {subtaskNames: subTaskName} });

export const getTaskGeneratorDebug = (taskName: string, taskType: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.get(`/tasks/generator/${taskName}/${taskType}/debug`, { headers: { ...headers, Accept: 'application/json' } });

export const getTaskTypeDebug = (taskType: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.get(`/tasks/${taskType}/debug?verbosity=1`, { headers: { ...headers, Accept: 'application/json' } });

export const getTables = (params): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.get(`/tables`, { params, headers: { ...headers, Accept: 'application/json' } });

export const getClusterConfig = (): Promise<AxiosResponse<ClusterConfig>> =>
  baseApi.get('/cluster/configs');

export const getQueryTables = (type?: string): Promise<AxiosResponse<QueryTables>> =>
  baseApi.get(`/tables${type ? `?type=${type}`: ''}`);

export const getTableSchema = (name: string): Promise<AxiosResponse<TableSchema>> =>
  baseApi.get(`/tables/${name}/schema`);

export const getQueryResult = (params: Object): Promise<AxiosResponse<SQLResult>> =>
  transformApi.post(`/sql`, params, {headers});

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
  baseApi.get(`/segments/${tableName}/metadata?type=${tableType}&columns=*`);

export const deleteSegment = (tableName: string, instanceName: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.delete(`/segments/${tableName}/${instanceName}`, {headers});

export const getTableJobs = (tableName: string, jobTypes?: string): Promise<AxiosResponse<TableSegmentJobs>> => {
  let queryParams = {};

  if (jobTypes) {
    queryParams["jobTypes"] = jobTypes
  }

  return baseApi.get(`/table/${tableName}/jobs`, { params: queryParams });
}

export const getSegmentReloadStatus = (jobId: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.get(`/segments/segmentReloadStatus/${jobId}`, {headers});

export const deleteTable = (tableName: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.delete(`/tables/${tableName}`, {headers});

export const deleteSchema = (schemaName: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.delete(`/schemas/${schemaName}`, {headers});

export const rebalanceServersForTable = (tableName: string, qParams: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.post(`/tables/${tableName}/rebalance?${qParams}`, null, {headers});

export const rebalanceBrokersForTable = (tableName: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.post(`/tables/${tableName}/rebuildBrokerResourceFromHelixTags`, null, {headers});

export const validateSchema = (schemaObject: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.post(`/schemas/validate`, schemaObject, {headers});

export const validateTable = (tableObject: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.post(`/tables/validate`, JSON.stringify(tableObject), {headers});

export const saveSchema = (schemaObject: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.post(`/schemas`, schemaObject, {headers});

export const saveTable = (tableObject: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.post(`/tables`, JSON.stringify(tableObject), {headers});

export const getState = (tableName: string, tableType: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.get(`/tables/${tableName}/state?type=${tableType}`);

export const getInfo = (): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.get(`/auth/info`);

export const authenticateUser = (authToken): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.get(`/auth/verify`, {headers:{"Authorization": authToken}});

export const getSegmentDebugInfo = (tableName: string, tableType: string): Promise<AxiosResponse<OperationResponse>> =>
  baseApi.get(`debug/tables/${tableName}?type=${tableType}&verbosity=10`);

export const getSegmentLevelDebugDetails = async (tableName: string, segmentName: string): Promise<SegmentDebugDetails> => {
  const response = await baseApiWithErrors.get(`debug/segments/${tableName}/${segmentName}`);
  return response.data;
}

export const requestTable = (): Promise<AxiosResponse<TableList>> =>
    baseApi.get(`/tables`);

export const requestUserList = (): Promise<AxiosResponse<UserList>> =>
    baseApi.get(`/users`);

export const requestAddUser = (userObject: UserObject): Promise<AxiosResponse<any>> =>
    baseApi.post('/users', JSON.stringify(userObject), {headers});

export const requestDeleteUser = (userObject: UserObject): Promise<AxiosResponse<any>> =>
    baseApi.delete(`/users/${userObject.username}?component=${userObject.component}`);

export const requestUpdateUser = (userObject: UserObject, passwordChanged: boolean): Promise<AxiosResponse<any>> =>
    baseApi.put(`/users/${userObject.username}?component=${userObject.component}&passwordChanged=${passwordChanged}`, JSON.stringify(userObject), {headers});