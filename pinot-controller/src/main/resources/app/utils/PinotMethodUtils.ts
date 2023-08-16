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

import jwtDecode from "jwt-decode";
import { get, map, each, isEqual, isArray, keys, union } from 'lodash';
import { DataTable, SQLResult } from 'Models';
import moment from 'moment';
import {
  getTenants,
  getInstances,
  getInstance,
  putInstance,
  setInstanceState,
  setTableState,
  dropInstance,
  getPeriodicTaskNames,
  getTaskTypes,
  getTaskTypeDebug,
  getTables,
  getTaskTypeTasks,
  getTaskTypeState,
  stopTasks,
  resumeTasks,
  cleanupTasks,
  deleteTasks,
  sheduleTask,
  executeTask,
  getJobDetail,
  getMinionMeta,
  getTasks,
  getTaskDebug,
  getTaskGeneratorDebug,
  updateInstanceTags,
  getClusterConfig,
  getQueryTables,
  getTableSchema,
  getQueryResult,
  getTenantTable,
  getTableSize,
  getIdealState,
  getExternalView,
  getTenantTableDetails,
  getSegmentMetadata,
  reloadSegment,
  getTableJobs,
  getClusterInfo,
  zookeeperGetList,
  zookeeperGetData,
  zookeeperGetListWithStat,
  zookeeperGetStat,
  zookeeperPutData,
  zookeeperDeleteNode,
  getBrokerListOfTenant,
  getServerListOfTenant,
  deleteSegment,
  putTable,
  putSchema,
  deleteTable,
  deleteSchema,
  reloadAllSegments,
  reloadStatus,
  rebalanceServersForTable,
  rebalanceBrokersForTable,
  validateSchema,
  validateTable,
  saveSchema,
  saveTable,
  getSchema,
  getSchemaList,
  getState,
  getInfo,
  authenticateUser,
  getSegmentDebugInfo,
  requestTable,
  requestUserList,
  requestAddUser,
  requestDeleteUser,
  requestUpdateUser,
  getTaskProgress,
  getSegmentReloadStatus,
  getTaskRuntimeConfig
} from '../requests';
import { baseApi } from './axios-config';
import Utils, { getDisplaySegmentStatus } from './Utils';
import { matchPath } from 'react-router';
import RouterData from '../router';
const JSONbig = require('json-bigint')({'storeAsString': true})

// This method is used to display tenants listing on cluster manager home page
// API: /tenants
// Expected Output: {columns: [], records: []}
const getTenantsData = () => {
  return getTenants().then(({ data }) => {
    const records = union(data.SERVER_TENANTS, data.BROKER_TENANTS);
    const serverPromiseArr = [], brokerPromiseArr = [], tablePromiseArr = [];
    const finalResponse = {
      columns: ['Tenant Name', 'Server', 'Broker', 'Tables'],
      records: []
    };
    records.map((record)=>{
      finalResponse.records.push([
        record
      ]);
      serverPromiseArr.push(getServerOfTenant(record));
      brokerPromiseArr.push(getBrokerOfTenant(record));
      tablePromiseArr.push(getTenantTable(record));
    });
    return Promise.all([
      Promise.all(serverPromiseArr),
      Promise.all(brokerPromiseArr),
      Promise.all(tablePromiseArr)
    ]).then((results)=>{
      const serversResponseData = results[0];
      const brokersResponseData = results[1];
      const tablesResponseData = results[2];

      tablesResponseData.map((tableResult, index)=>{
        const serverCount = serversResponseData[index]?.length || 0;
        const brokerCount = brokersResponseData[index]?.length || 0;
        const tablesCount = tableResult.data.tables.length
        finalResponse.records[index].push(serverCount, brokerCount, tablesCount);
      });
      return finalResponse;
    });
  });
};

// This method is used to fetch all instances on cluster manager home page
// API: /instances
// Expected Output: {Controller: ['Controller1', 'Controller2'], Broker: ['Broker1', 'Broker2']}
const getAllInstances = () => {
  return getInstances().then(({ data }) => {
    const initialVal: DataTable = {};
    // It will create instances list array like
    // {Controller: ['Controller1', 'Controller2'], Broker: ['Broker1', 'Broker2']}
    const groupedData = data.instances.reduce((r, a) => {
      const y = a.split('_');
      const key = y[0].trim();
      r[key] = [...(r[key] || []), a];
      return r;
    }, initialVal);
    return {'Controller': groupedData.Controller, ...groupedData};
  });
};

// This method is used to display instance data on cluster manager home page
// API: /instances/:instanceName
// Expected Output: {columns: [], records: []}
const getInstanceData = (instances, liveInstanceArr) => {
  const promiseArr = [...instances.map((inst) => getInstance(inst))];

  return Promise.all(promiseArr).then((result) => {
    return {
      columns: ['Instance Name', 'Enabled', 'Hostname', 'Port', 'Status'],
      records: [
        ...result.map(({ data }) => [
          data.instanceName,
          data.enabled,
          data.hostName,
          data.port,
          liveInstanceArr.indexOf(data.instanceName) > -1 ? 'Alive' : 'Dead'
        ]),
      ],
    };
  });
};

// This method is used to fetch cluster name
// API: /cluster/info
// Expected Output: {clusterName: ''}
const getClusterName = () => {
  return getClusterInfo().then(({ data }) => {
    return data.clusterName;
  });
};

// This method is used to fetch array of live instances name
// API: /zk/ls?path=:ClusterName/LIVEINSTANCES
// Expected Output: []
const getLiveInstance = (clusterName) => {
  const params = encodeURIComponent(`/${clusterName}/LIVEINSTANCES`);
  return zookeeperGetList(params).then((data) => {
    return data;
  });
};

// This method is used to diaplay cluster congifuration on cluster manager home page
// API: /cluster/configs
// Expected Output: {columns: [], records: []}
const getClusterConfigData = () => {
  return getClusterConfig().then(({ data }) => {
    return {
      columns: ['Property', 'Value'],
      records: [...Object.keys(data).map((key) => [key, data[key]])],
    };
  });
};

// This method is used to fetch cluster congifuration
// API: /cluster/configs
// Expected Output: {key: value}
const getClusterConfigJSON = () => {
  return getClusterConfig().then(({ data }) => {
    return data;
  });
};

// This method is used to display table listing on query page
// API: /tables
// Expected Output: {columns: [], records: []}
const getQueryTablesList = ({bothType = false}) => {
  const promiseArr = bothType ? [getQueryTables('realtime'), getQueryTables('offline')] : [getQueryTables()];

  return Promise.all(promiseArr).then((results) => {
    const responseObj = {
      columns: ['Tables'],
      records:  []
    };
    results.map((result)=>{
      result.data.tables.map((table)=>{
        responseObj.records.push([table]);
      });
    });
    return responseObj;
  });
};

// This method is used to display particular table schema on query page
// API: /tables/:tableName/schema
const getTableSchemaData = (tableName) => {
  return getTableSchema(tableName).then(({ data }) => {
    return data;
  });
};

const getAsObject = (str: SQLResult) => {
  if (typeof str === 'string' || str instanceof String) {
    try {
      return JSONbig.parse(str);
    } catch(e) {
      return JSON.parse(JSON.stringify(str));
    }
  }
  return str;
};

// This method is used to display query output in tabular format as well as JSON format on query page
// API: /:urlName (Eg: sql or pql)
// Expected Output: {columns: [], records: []}
const getQueryResults = (params) => {
  return getQueryResult(params).then(({ data }) => {
    let queryResponse = getAsObject(data);

    let errorStr = '';
    let dataArray = [];
    let columnList = [];
    // if sql api throws error, handle here
    if(typeof queryResponse === 'string'){
      errorStr = queryResponse;
    } 
    if (queryResponse && queryResponse.exceptions && queryResponse.exceptions.length) {
      try{
        errorStr = JSON.stringify(queryResponse.exceptions, null, 2);
      } catch {
        errorStr = "";
      }
    } 
    if (queryResponse.resultTable?.dataSchema?.columnNames?.length) {
      columnList = queryResponse.resultTable.dataSchema.columnNames;
      dataArray = queryResponse.resultTable.rows;
    }

    const columnStats = ['timeUsedMs',
      'numDocsScanned',
      'totalDocs',
      'numServersQueried',
      'numServersResponded',
      'numSegmentsQueried',
      'numSegmentsProcessed',
      'numSegmentsMatched',
      'numConsumingSegmentsQueried',
      'numEntriesScannedInFilter',
      'numEntriesScannedPostFilter',
      'numGroupsLimitReached',
      'partialResponse',
      'minConsumingFreshnessTimeMs',
      'offlineThreadCpuTimeNs',
      'realtimeThreadCpuTimeNs',
      'offlineSystemActivitiesCpuTimeNs',
      'realtimeSystemActivitiesCpuTimeNs',
      'offlineResponseSerializationCpuTimeNs',
      'realtimeResponseSerializationCpuTimeNs',
      'offlineTotalCpuTimeNs',
      'realtimeTotalCpuTimeNs'
    ];

    return {
      error: errorStr,
      result: {
        columns: columnList,
        records: dataArray,
      },
      queryStats: {
        columns: columnStats,
        records: [[queryResponse.timeUsedMs, queryResponse.numDocsScanned, queryResponse.totalDocs, queryResponse.numServersQueried, queryResponse.numServersResponded,
          queryResponse.numSegmentsQueried, queryResponse.numSegmentsProcessed, queryResponse.numSegmentsMatched, queryResponse.numConsumingSegmentsQueried,
          queryResponse.numEntriesScannedInFilter, queryResponse.numEntriesScannedPostFilter, queryResponse.numGroupsLimitReached,
          queryResponse.partialResponse ? queryResponse.partialResponse : '-', queryResponse.minConsumingFreshnessTimeMs,
          queryResponse.offlineThreadCpuTimeNs, queryResponse.realtimeThreadCpuTimeNs,
          queryResponse.offlineSystemActivitiesCpuTimeNs, queryResponse.realtimeSystemActivitiesCpuTimeNs,
          queryResponse.offlineResponseSerializationCpuTimeNs, queryResponse.realtimeResponseSerializationCpuTimeNs,
          queryResponse.offlineTotalCpuTimeNs, queryResponse.realtimeTotalCpuTimeNs]]
      },
      data: queryResponse,
    };
  });
};

// This method is used to display table data of a particular tenant
// API: /tenants/:tenantName/tables
//      /tables/:tableName/size
//      /tables/:tableName/idealstate
//      /tables/:tableName/externalview
// Expected Output: {columns: [], records: []}
const getTenantTableData = (tenantName) => {
  return getTenantTable(tenantName).then(({ data }) => {
    const tableArr = data.tables.map((table) => table);
    return getAllTableDetails(tableArr);
  });
};

const getSchemaObject = async (schemaName) =>{
  let schemaObj:Array<any> = [];
    let {data} = await getSchema(schemaName);
    console.log(data);
      schemaObj.push(data.schemaName);
      schemaObj.push(data.dimensionFieldSpecs ? data.dimensionFieldSpecs.length : 0);
      schemaObj.push(data.dateTimeFieldSpecs ? data.dateTimeFieldSpecs.length : 0);
      schemaObj.push(data.metricFieldSpecs ? data.metricFieldSpecs.length : 0);
      schemaObj.push(schemaObj[1] + schemaObj[2] + schemaObj[3]);
      return schemaObj;
  }

// This method is used to display schema listing on the tables listing page
// API: /schemas
// Expected Output: {columns: [], records: []}
const getListingSchemaList = () => {
  return getSchemaList().then((results) => {
    const responseObj = {
      columns: ['Schemas'],
      records: []
    };
    results.data.forEach((result)=>{
      responseObj.records.push([result]);
    });
    return responseObj;
  })
};

const allSchemaDetailsColumnHeader = ["Schema Name", "Dimension Columns", "Date-Time Columns", "Metrics Columns", "Total Columns"];

const getAllSchemaDetails = async (schemaList) => {
  let schemaDetails:Array<any> = [];
  let promiseArr = [];
  promiseArr = schemaList.map(async (o)=>{
    return await getSchema(o);
  });
  const results = await Promise.all(promiseArr);
  schemaDetails = results.map((obj)=>{
    let schemaObj = [];
    schemaObj.push(obj.data.schemaName);
    schemaObj.push(obj.data.dimensionFieldSpecs ? obj.data.dimensionFieldSpecs.length : 0);
    schemaObj.push(obj.data.dateTimeFieldSpecs ? obj.data.dateTimeFieldSpecs.length : 0);
    schemaObj.push(obj.data.metricFieldSpecs ? obj.data.metricFieldSpecs.length : 0);
    schemaObj.push(schemaObj[1] + schemaObj[2] + schemaObj[3]);
    return schemaObj;
  })
  return {
    columns: allSchemaDetailsColumnHeader,
    records: schemaDetails
  };
}

const allTableDetailsColumnHeader = [
  'Table Name',
  'Reported Size',
  'Estimated Size',
  'Number of Segments',
  'Status',
];

const getAllTableDetails = (tablesList) => {
  if (tablesList.length) {
    const promiseArr = [];
    tablesList.map((name) => {
      promiseArr.push(getTableSize(name));
      promiseArr.push(getIdealState(name));
      promiseArr.push(getExternalView(name));
    });

    return Promise.all(promiseArr).then((results) => {
      const finalRecordsArr = [];
      let singleTableData = [];
      let idealStateObj = null;
      let externalViewObj = null;
      results.map((result, index) => {
        // since we have 3 promises, we are using mod 3 below
        if (index % 3 === 0) {
          // response of getTableSize API
          const {
            tableName,
            reportedSizeInBytes,
            estimatedSizeInBytes,
          } = result.data;
          singleTableData.push(
            tableName,
            Utils.formatBytes(reportedSizeInBytes),
            Utils.formatBytes(estimatedSizeInBytes)
          );
        } else if (index % 3 === 1) {
          // response of getIdealState API
          idealStateObj = result.data.OFFLINE || result.data.REALTIME || {};
        } else if (index % 3 === 2) {
          // response of getExternalView API
          externalViewObj = result.data.OFFLINE || result.data.REALTIME || {};
          const externalSegmentCount = Object.keys(externalViewObj).length;
          const idealSegmentCount = Object.keys(idealStateObj).length;
          // Generating data for the record
          singleTableData.push(
            `${externalSegmentCount} / ${idealSegmentCount}`,
            Utils.getSegmentStatus(idealStateObj, externalViewObj)
          );
          // saving into records array
          finalRecordsArr.push(singleTableData);
          // resetting the required variables
          singleTableData = [];
          idealStateObj = null;
          externalViewObj = null;
        }
      });
      return {
        columns: allTableDetailsColumnHeader,
        records: finalRecordsArr,
      };
    });
  }
  return {
    columns: allTableDetailsColumnHeader,
    records: []
  };
};

// This method is used to display summary of a particular tenant table
// API: /tables/:tableName/size
// Expected Output: {tableName: '', reportedSize: '', estimatedSize: ''}
const getTableSummaryData = (tableName) => {
  return getTableSize(tableName).then(({ data }) => {
    return {
      tableName: data.tableName,
      reportedSize: data.reportedSizeInBytes,
      estimatedSize: data.estimatedSizeInBytes,
    };
  });
};

// This method is used to display segment list of a particular tenant table
// API: /tables/:tableName/idealstate
//      /tables/:tableName/externalview
// Expected Output: {columns: [], records: [], externalViewObject: {}}
const getSegmentList = (tableName) => {
  const promiseArr = [];
  promiseArr.push(getIdealState(tableName));
  promiseArr.push(getExternalView(tableName));

  return Promise.all(promiseArr).then((results) => {
    const idealStateObj = results[0].data.OFFLINE || results[0].data.REALTIME;
    const externalViewObj = results[1].data.OFFLINE || results[1].data.REALTIME;

    return {
      columns: ['Segment Name', 'Status'],
      records: Object.keys(idealStateObj).map((key) => {
        return [
          key,
          getDisplaySegmentStatus(idealStateObj[key], externalViewObj[key])
        ];
      }),
      externalViewObj
    };
  });
};

const getSegmentStatus = (idealSegment, externalViewSegment) => {
  if(isEqual(idealSegment, externalViewSegment)){
    return 'Good';
  }
  let goodCount = 0;
  // There is a possibility that the segment is in ideal state but not in external view
  // making external view segment as null.
  const totalCount = externalViewSegment ? Object.keys(externalViewSegment).length : 0;
  Object.keys(idealSegment).map((replicaName)=>{
    const idealReplicaState = idealSegment[replicaName];
    const externalReplicaState = externalViewSegment ? externalViewSegment[replicaName] : '';
    if(idealReplicaState === externalReplicaState || (externalReplicaState === 'CONSUMING')){
      goodCount += 1;
    }
  });
  if(goodCount === 0 || totalCount === 0){
    return 'Bad';
  } else if(goodCount === totalCount){
    return  'Good';
  } else {
    return `Partial-${goodCount}/${totalCount}`;
  }
};

// This method is used to display JSON format of a particular tenant table
// API: /tables/:tableName/idealstate
//      /tables/:tableName/externalview
// Expected Output: {columns: [], records: []}
const getTableDetails = (tableName) => {
  return getTenantTableDetails(tableName).then(({ data }) => {
    return data;
  });
};

// This method is used to display summary of a particular segment, replica set as well as JSON format of a tenant table
// API: /tables/tableName/externalview
//      /segments/:tableName/:segmentName/metadata
// Expected Output: {columns: [], records: []}
const getSegmentDetails = (tableName, segmentName) => {
  let [baseTableName, tableType] = Utils.splitStringByLastUnderscore(tableName)
  const promiseArr = [];
  promiseArr.push(getExternalView(tableName));
  promiseArr.push(getSegmentMetadata(tableName, segmentName));
  promiseArr.push(getSegmentDebugInfo(baseTableName, tableType.toLowerCase()));

  return Promise.all(promiseArr).then((results) => {
    const obj = results[0].data.OFFLINE || results[0].data.REALTIME;
    const segmentMetaData = results[1].data;
    const debugObj = results[2].data;
    let debugInfoObj = {};

    if(debugObj && debugObj[0]){
      const debugInfosObj = debugObj[0].segmentDebugInfos?.find((o)=>{return o.segmentName === segmentName});
      if(debugInfosObj){
        const serverNames = keys(debugInfosObj?.serverState || {});
        serverNames?.map((serverName)=>{
          debugInfoObj[serverName] = debugInfosObj.serverState[serverName]?.errorInfo?.errorMessage;
        });
      }
    }
    console.log(debugInfoObj);

    const result = [];
    for (const prop in obj[segmentName]) {
      if (obj[segmentName]) {
        const status = obj[segmentName][prop];
        result.push([prop, status === 'ERROR' ? {value: status, tooltip: debugInfoObj[prop]} : status]);
      }
    }

    const segmentMetaDataJson = { ...segmentMetaData }
    delete segmentMetaDataJson.indexes
    delete segmentMetaDataJson.columns
    const indexes = get(segmentMetaData, 'indexes', {})

    return {
      replicaSet: {
        columns: ['Server Name', 'Status'],
        records: [...result],
      },
      indexes: {
        columns: ['Field Name', 'Bloom Filter', 'Dictionary', 'Forward Index', 'Sorted', 'Inverted Index', 'JSON Index', 'Null Value Vector Reader', 'Range Index'],
        records: Object.keys(indexes).map(fieldName => [
          fieldName,
          segmentMetaData.indexes[fieldName]["bloom-filter"] === "YES",
          segmentMetaData.indexes[fieldName]["dictionary"] === "YES",
          segmentMetaData.indexes[fieldName]["forward-index"] === "YES",
          ((segmentMetaData.columns || []).filter(row => row.columnName === fieldName)[0] || {sorted: false}).sorted,
          segmentMetaData.indexes[fieldName]["inverted-index"] === "YES",
          segmentMetaData.indexes[fieldName]["json-index"] === "YES",
          segmentMetaData.indexes[fieldName]["null-value-vector-reader"] === "YES",
          segmentMetaData.indexes[fieldName]["range-index"] === "YES",
        ])
      },
      summary: {
        segmentName,
        totalDocs: segmentMetaData['segment.total.docs'],
        createTime: moment(+segmentMetaData['segment.creation.time']).format(
          'MMMM Do YYYY, h:mm:ss'
        ),
      },
      JSON: segmentMetaDataJson
    };
  });
};

// This method is used to fetch the LIVEINSTANCE config
// API: /zk/get?path=:clusterName/LIVEINSTANCES/:instanceName
// Expected Output: configuration in JSON format
const getLiveInstanceConfig = (clusterName, instanceName) => {
  const params = encodeURIComponent(`/${clusterName}/LIVEINSTANCES/${instanceName}`);
  return zookeeperGetData(params).then((res) => {
    return res.data;
  });
};

// This method is used to fetch the instance config
// API: /zk/get?path=:clusterName/CONFIGS/PARTICIPANT/:instanceName
// Expected Output: configuration in JSON format
const getInstanceConfig = (clusterName, instanceName) => {
  const params = encodeURIComponent(`/${clusterName}/CONFIGS/PARTICIPANT/${instanceName}`);
  return zookeeperGetData(params).then((res) => {
    return res.data;
  });
};

// This method is used to get instance info
// API: /instances/:instanceName
const getInstanceDetails = (instanceName) => {
  return getInstance(instanceName).then((res)=>{
    return res.data;
  });
};

const updateInstanceDetails = (instanceName, instanceDetails) => {
  return putInstance(instanceName, instanceDetails).then((res)=>{
    return res.data;
  })
};

// This method is responsible to prepare the data for tree structure
// It internally calls getNodeData() which makes the required API calls.
const getZookeeperData = (path, count) => {
  let counter = count;
  const newTreeData = [{
    nodeId: `${counter++}`,
    label: path,
    child: [],
    isLeafNode: false,
    hasChildRendered: true
  }];
  return getNodeData(path).then((obj)=>{
    const { currentNodeData, currentNodeMetadata, currentNodeListStat } = obj;
    const pathNames = Object.keys(currentNodeListStat);
    pathNames.map((pathName)=>{
      newTreeData[0].child.push({
        nodeId: `${counter++}`,
        label: pathName,
        fullPath: path === '/' ? path+pathName : `${path}/${pathName}`,
        child: [],
        isLeafNode: currentNodeListStat[pathName].numChildren === 0,
        hasChildRendered: false
      });
    });
    return { newTreeData, currentNodeData, currentNodeMetadata, currentNodeListStat, counter };
  });
};

// This method is responsible to get data, get list with stats and get stats.
// API: /zk/get => Get node data
// API: /zk/lsl => Get node list with stats
// API: /zk/get => Get node stats
const getNodeData = (path) => {
  const params = encodeURIComponent(path);
  const promiseArr = [
    zookeeperGetData(params),
    zookeeperGetListWithStat(params),
    zookeeperGetStat(params)
  ];
  return Promise.all(promiseArr).then((results)=>{
    const currentNodeData = results[0].data || {};
    const currentNodeListStat = results[1].data;
    const currentNodeMetadata = results[2].data;

    if(currentNodeMetadata.ctime || currentNodeMetadata.mtime){
      currentNodeMetadata.ctime = moment(+currentNodeMetadata.ctime).format(
        'MMMM Do YYYY, h:mm:ss'
      );
      currentNodeMetadata.mtime = moment(+currentNodeMetadata.mtime).format(
        'MMMM Do YYYY, h:mm:ss'
      );
    }
    return { currentNodeData, currentNodeMetadata, currentNodeListStat };
  });
};

const putNodeData = (data) => {
  const serializedData = Utils.serialize(data);
  return zookeeperPutData(serializedData).then((obj)=>{
    return obj;
  });
};

const deleteNode = (path) => {
  const params = encodeURIComponent(path);
  return zookeeperDeleteNode(params).then((obj)=>{
    return obj;
  });
};

const getBrokerOfTenant = (tenantName) => {
  return getBrokerListOfTenant(tenantName).then((response)=>{
    return !response.data.error ? response.data : [];
  });
};

const getServerOfTenant = (tenantName) => {
  return getServerListOfTenant(tenantName).then((response)=>{
    return !response.data.error ? response.data.ServerInstances : [];
  });
};

const updateTags = (instanceName, tagsList) => {
  return updateInstanceTags(instanceName, tagsList.toString()).then((response)=>{
    return response.data;
  });
};

const toggleInstanceState = (instanceName, state) => {
  return setInstanceState(instanceName, state).then((response)=>{
    return response.data;
  });
};

const toggleTableState = (tableName, state, tableType) => {
  return setTableState(tableName, state, tableType).then((response)=>{
    return response.data;
  });
};

const deleteInstance = (instanceName) => {
  return dropInstance(instanceName).then((response)=>{
    return response.data;
  });
};

const getAllPeriodicTaskNames = () => {
  return getPeriodicTaskNames().then((response)=>{
    return { columns: ['Task Name'], records: response.data.map(d => [d]) };
  });
};

const getAllTaskTypes = async () => {
  const finalResponse = {
    columns: ['Task Type', 'Num Tasks in Queue', 'Queue Status'],
    records: []
  }
  await new Promise((resolve, reject) => {
    getTaskTypes().then(async (response)=>{
      if (isArray(response.data)) {
        const promiseArr = [];
        const fetchInfo = async (taskType) => {
          const [ count, state ] = await getTaskInfo(taskType);
          finalResponse.records.push([taskType, count, state]);
        };
        response.data.forEach((taskType) => promiseArr.push(fetchInfo(taskType)));
        await Promise.all(promiseArr);
        resolve(finalResponse);
      }
    });
  })
  return finalResponse;
};

const getTaskInfo = async (taskType) => {
  const tasksRes = await getTaskTypeTasks(taskType);
  const stateRes = await getTaskTypeState(taskType);
  const state = get(stateRes, 'data', '');
  return [tasksRes?.data?.length || 0, state];
};

const stopAllTasks = (taskType) => {
  return stopTasks(taskType).then((response)=>{
    return response.data;
  });
};

const resumeAllTasks = (taskType) => {
  return resumeTasks(taskType).then((response)=>{
    return response.data;
  });
};

const cleanupAllTasks = (taskType) => {
  return cleanupTasks(taskType).then((response)=>{
    return response.data;
  });
};

const deleteAllTasks = (taskType) => {
  return deleteTasks(taskType).then((response)=>{
    return response.data;
  });
};

const getMinionMetaData = (tableName, taskType) => {
  return getMinionMeta(tableName, taskType).then((response)=>{
    return response.data;
  });
};

const getElapsedTime = (startTime) => {
  const currentTime = moment();
  const diff = currentTime.diff(startTime);
  const elapsedTime = diff > (1000 * 60 * 60) ? `${currentTime.diff(startTime, 'hour')} hours` : `${currentTime.diff(startTime, 'minute')} minutes`;
  return elapsedTime;
}

const getTasksList = async (tableName, taskType) => {
  const finalResponse = {
    columns: ['Task ID', 'Status', 'Start Time', 'Finish Time', 'Num of Sub Tasks'],
    records: []
  }
  await new Promise((resolve, reject) => {
    getTasks(tableName, taskType).then(async (response)=>{
      const promiseArr = [];
      const fetchInfo = async (taskID, status) => {
        const debugData = await getTaskDebugData(taskID);
        finalResponse.records.push([
          taskID,
          status,
          get(debugData, 'data.startTime', ''),
          get(debugData, 'data.finishTime', ''),
          get(debugData, 'data.subtaskCount.total', 0)
        ]);
      };
      each(response.data, async (val, key) => {
        promiseArr.push(fetchInfo(key, val));
      });
      await Promise.all(promiseArr);
      resolve(finalResponse);
    });
  })
  return finalResponse;
};

const getTaskRuntimeConfigData = async (taskName: string) => {
  const response = await getTaskRuntimeConfig(taskName);
  
  return response.data;
}

const getTaskDebugData = async (taskName) => {
  const debugRes = await getTaskDebug(taskName);
  return debugRes;
};

const getTaskProgressData = async (taskName, subTaskName) => {
  const progressData = await getTaskProgress(taskName, subTaskName);

  return progressData.data;
}

const getTaskGeneratorDebugData = async (taskName, taskType) => {
  const debugRes = await getTaskGeneratorDebug(taskName, taskType);
  return debugRes;
};

const reloadSegmentOp = (tableName, segmentName) => {
  return reloadSegment(tableName, segmentName).then((response)=>{
    return response.data;
  });
};

const reloadAllSegmentsOp = (tableName, tableType) => {
  return reloadAllSegments(tableName, tableType).then((response)=>{
    return response.data;
  });
};

const reloadStatusOp = (tableName, tableType) => {
  return reloadStatus(tableName, tableType).then((response)=>{
    return response.data;
  });
}

const deleteSegmentOp = (tableName, segmentName) => {
  return deleteSegment(tableName, segmentName).then((response)=>{
    return response.data;
  });
};

const fetchTableJobs = async (tableName: string, jobTypes?: string) => {
  const response = await getTableJobs(tableName, jobTypes);
  
  return response.data;
}

const fetchSegmentReloadStatus = async (jobId: string) => {
  const response = await getSegmentReloadStatus(jobId);
  
  return response.data;
}

const updateTable = (tableName: string, table: string) => {
  return putTable(tableName, table).then((res)=>{
    return res.data;
  })
};

const updateSchema = (schemaName: string, schema: string, reload?: boolean) => {
  return putSchema(schemaName, schema, reload).then((res)=>{
    return res.data;
  })
};

const deleteTableOp = (tableName) => {
  return deleteTable(tableName).then((response)=>{
    return response.data;
  });
};

const deleteSchemaOp = (tableName) => {
  return deleteSchema(tableName).then((response)=>{
    return response.data;
  });
};

const rebalanceServersForTableOp = (tableName, queryParams) => {
  const q_params = Utils.serialize(queryParams);
  return rebalanceServersForTable(tableName, q_params).then((response)=>{
    return  response.data;
  });
};

const rebalanceBrokersForTableOp = (tableName) => {
  return rebalanceBrokersForTable(tableName).then((response)=>{
    return response.data;
  });
};

const validateSchemaAction = (schemaObj) => {
  return validateSchema(schemaObj).then((response)=>{
    return response.data;
  });
};

const validateTableAction = (tableObj) => {
  return validateTable(tableObj).then((response)=>{
    return response.data;
  });
};

const saveSchemaAction = (schemaObj) => {
  return saveSchema(schemaObj).then((response)=>{
    return response.data;
  });
};

const saveTableAction = (tableObj) => {
  return saveTable(tableObj).then((response)=>{
    return response.data;
  });
};

const getSchemaData = (schemaName) => {
  return getSchema(schemaName).then((response)=>{
    return response.data;
  });
};

const getTableState = (tableName, tableType) => {
  return getState(tableName, tableType).then((response)=>{
    return response.data;
  });
};

const getAuthInfo = () => {
  return getInfo().then((response)=>{
    return response.data;
  });
};

const getWellKnownOpenIdConfiguration = (issuer) => {
  return baseApi
    .get(`${issuer}/.well-known/openid-configuration`)
    .then((response) => {
      return response.data;
    });
};

const verifyAuth = (authToken) => {
  return authenticateUser(authToken).then((response)=>{
    return response.data;
  });
};

const getAccessTokenFromHashParams = () => {
  let accessToken = '';
  const hashParam = removeAllLeadingForwardSlash(location.hash.substring(1));
  
  const urlSearchParams = new URLSearchParams(hashParam);
  if (urlSearchParams.has('access_token')) {
    accessToken = urlSearchParams.get('access_token') as string;
  }

  return accessToken;
};

const removeAllLeadingForwardSlash = (string: string) => {
  if(!string) {
    return "";
  }

  return string.replace(new RegExp("^/+", "g"), "");
}

// validates app redirect path with known routes
const validateRedirectPath = (path: string): boolean => {
  if(!path) {
    return false;
  }

  if(!path.startsWith("/")) {
    path = "/" + path;
  }

  let pathName = "";

  try {
    const appUrl = new URL(location.origin + path);
    pathName = appUrl.pathname;
  } catch(err) {
    console.error(err);
    return false;
  }

  const knownAppRoutes = RouterData.map((data) => data.path);
  const routeMatches = matchPath(pathName, {path: knownAppRoutes, exact: true});
  
  if(!routeMatches) {
    return false;
  }

  return true;
};

const getURLWithoutAccessToken = (fallbackUrl = '/'): string => {
  let prefix = '';
  let url = location.hash.substring(1);
  if (url.includes('access_token=')) {
    if (url.startsWith('/')) {
      prefix = '/';
      url = url.substring(1);
    }

    const urlSearchParams = new URLSearchParams(url);
    urlSearchParams.delete('access_token');
    const urlParams = [];

    // Loop over to get all params with empty value
    for (const [key, value] of urlSearchParams.entries()) {
      if (!value) {
        urlParams.push(key);
      }
    }

    // Loop over to delete all params with empty value
    for (const key of urlParams){
        urlSearchParams.delete(key);
    }

    // Check if any param remains, prepend it to urlParam as string
    if(urlSearchParams.toString()){
      urlParams.unshift(urlSearchParams.toString());
    }
    
    url = urlParams.join('&');

    if(!validateRedirectPath(url)) {
      // constructed redirect url is not a valid app route
      // redirect to fallBackUrl
      url = fallbackUrl;
    }
  } else {
    url = fallbackUrl;
  }
  return `${prefix}${url}`;
};

const getTable = ()=>{
  return requestTable().then(response=>{
    return response.data;
  })
};

const getTaskTypeDebugData = (taskType)=>{
  return getTaskTypeDebug(taskType).then(response=>{
    return response.data;
  })
};

const getTableData = (params)=>{
  return getTables(params).then(response=>{
    return response.data;
  })
};

const scheduleTaskAction = (tableName, taskType)=>{
  return sheduleTask(tableName, taskType).then(response=>{
    return response.data;
  })
};

const executeTaskAction = (data)=>{
  return executeTask(data).then(response=>{
    return response.data;
  })
};

const getScheduleJobDetail = (tableName, taskType)=>{
  return getJobDetail(tableName, taskType).then(response=>{
    return response.data;
  })
};

const getUserList = ()=>{
  return requestUserList().then(response=>{
    return response.data;
  })
};

const addUser = (userObject)=>{
  return requestAddUser(userObject).then(response=>{
    return response.data;
  })
};

const deleteUser = (userObject)=>{
  return requestDeleteUser(userObject).then(response=>{
    return response.data;
  })
};

const updateUser = (userObject, passwordChanged) =>{
  return requestUpdateUser(userObject, passwordChanged).then(response=>{
    return response.data;
  })
}

const getAuthUserNameFromAccessToken = (
  accessToken: string
): string => {
  if (!accessToken) {
      return "";
  }

  let decoded;
  try {
      decoded = jwtDecode(accessToken);
  } catch (e) {
      return "";
  }

  if (!decoded) {
      return "";
  }

  const name = get(decoded, "name") || "";
  return name;
};

const getAuthUserEmailFromAccessToken = (
  accessToken: string
): string => {
  if (!accessToken) {
      return "";
  }

  let decoded;
  try {
      decoded = jwtDecode(accessToken);
  } catch (e) {
      return "";
  }

  if (!decoded) {
      return "";
  }

  const email =
      get(decoded, "email") || "";

  return email;
};

export default {
  getTenantsData,
  getAllInstances,
  getInstanceData,
  getClusterConfigData,
  getClusterConfigJSON,
  getQueryTablesList,
  getTableSchemaData,
  getQueryResults,
  getTenantTableData,
  allTableDetailsColumnHeader,
  getAllTableDetails,
  getTableSummaryData,
  getSegmentList,
  getSegmentStatus,
  getTableDetails,
  getSegmentDetails,
  getClusterName,
  getLiveInstance,
  getLiveInstanceConfig,
  getInstanceConfig,
  getInstanceDetails,
  updateInstanceDetails,
  getZookeeperData,
  getNodeData,
  putNodeData,
  deleteNode,
  getBrokerOfTenant,
  getServerOfTenant,
  updateTags,
  toggleInstanceState,
  toggleTableState,
  deleteInstance,
  getAllPeriodicTaskNames,
  getAllTaskTypes,
  fetchTableJobs,
  fetchSegmentReloadStatus,
  getTaskTypeDebugData,
  getTableData,
  getTaskRuntimeConfigData,
  getTaskInfo,
  stopAllTasks,
  resumeAllTasks,
  cleanupAllTasks,
  deleteAllTasks,
  scheduleTaskAction,
  executeTaskAction,
  getScheduleJobDetail,
  getMinionMetaData,
  getElapsedTime,
  getTasksList,
  getTaskDebugData,
  getTaskProgressData,
  getTaskGeneratorDebugData,
  deleteSegmentOp,
  reloadSegmentOp,
  reloadStatusOp,
  reloadAllSegmentsOp,
  updateTable,
  updateSchema,
  deleteTableOp,
  deleteSchemaOp,
  rebalanceServersForTableOp,
  rebalanceBrokersForTableOp,
  validateSchemaAction,
  validateTableAction,
  saveSchemaAction,
  saveTableAction,
  getSchemaData,
  getQuerySchemaList: getListingSchemaList,
  allSchemaDetailsColumnHeader,
  getAllSchemaDetails,
  getTableState,
  getAuthInfo,
  getWellKnownOpenIdConfiguration,
  verifyAuth,
  getAccessTokenFromHashParams,
  getURLWithoutAccessToken,
  getTable,
  getUserList,
  addUser,
  deleteUser,
  updateUser,
  getAuthUserNameFromAccessToken,
  getAuthUserEmailFromAccessToken
};
