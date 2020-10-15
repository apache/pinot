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

import _ from 'lodash';
import { DataTable, SQLResult } from 'Models';
import moment from 'moment';
import {
  getTenants,
  getInstances,
  getInstance,
  putInstance,
  setInstanceState,
  dropInstance,
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
  getClusterInfo,
  zookeeperGetList,
  zookeeperGetData,
  zookeeperGetListWithStat,
  zookeeperGetStat,
  zookeeperPutData,
  zookeeperDeleteNode,
  getBrokerListOfTenant,
  getServerListOfTenant,
  deleteSegment
} from '../requests';
import Utils from './Utils';

// This method is used to display tenants listing on cluster manager home page
// API: /tenants
// Expected Output: {columns: [], records: []}
const getTenantsData = () => {
  return getTenants().then(({ data }) => {
    const records = _.union(data.SERVER_TENANTS, data.BROKER_TENANTS);
    const promiseArr = [];
    const finalResponse = {
      columns: ['Tenant Name', 'Server', 'Broker', 'Tables'],
      records: []
    };
    records.map((record)=>{
      finalResponse.records.push([
        record,
        data.SERVER_TENANTS.indexOf(record) > -1 ? 1 : 0,
        data.BROKER_TENANTS.indexOf(record) > -1 ? 1 : 0
      ]);
      promiseArr.push(getTenantTable(record));
    });
    return Promise.all(promiseArr).then((results)=>{
      results.map((result, index)=>{
        finalResponse.records[index].push(result.data.tables.length);
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
// API: /instances/:instaneName
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
// Expected Output: {columns: [], records: []}
const getTableSchemaData = (tableName, showFieldType) => {
  return getTableSchema(tableName).then(({ data }) => {
    const dimensionFields = data.dimensionFieldSpecs || [];
    const metricFields = data.metricFieldSpecs || [];
    const dateTimeField = data.dateTimeFieldSpecs || [];

    dimensionFields.map((field) => {
      field.fieldType = 'Dimension';
    });

    metricFields.map((field) => {
      field.fieldType = 'Metric';
    });

    dateTimeField.map((field) => {
      field.fieldType = 'Date-Time';
    });
    const columnList = [...dimensionFields, ...metricFields, ...dateTimeField];
    if (showFieldType) {
      return {
        columns: ['column', 'type', 'Field Type'],
        records: columnList.map((field) => {
          return [field.name, field.dataType, field.fieldType];
        }),
      };
    }
    return {
      columns: ['column', 'type'],
      records: columnList.map((field) => {
        return [field.name, field.dataType];
      }),
    };
  });
};

const getAsObject = (str: SQLResult) => {
  if (typeof str === 'string' || str instanceof String) {
    return JSON.parse(JSON.stringify(str));
  }
  return str;
};

// This method is used to display query output in tabular format as well as JSON format on query page
// API: /:urlName (Eg: sql or pql)
// Expected Output: {columns: [], records: []}
const getQueryResults = (params, url, checkedOptions) => {
  return getQueryResult(params, url).then(({ data }) => {
    let queryResponse = null;

    queryResponse = getAsObject(data);

    // if sql api throws error, handle here
    if(typeof queryResponse === 'string'){
      return {error: queryResponse};
    } else if(queryResponse.exceptions.length){
      return {error: JSON.stringify(queryResponse.exceptions, null, 2)};
    }

    let dataArray = [];
    let columnList = [];
    if (checkedOptions.querySyntaxPQL === true) {
      if (queryResponse) {
        if (queryResponse.selectionResults) {
          // Selection query
          columnList = queryResponse.selectionResults.columns;
          dataArray = queryResponse.selectionResults.results;
        } else if (!queryResponse.aggregationResults[0]?.groupByResult) {
          // Simple aggregation query
          columnList = _.map(
            queryResponse.aggregationResults,
            (aggregationResult) => {
              return { title: aggregationResult.function };
            }
          );

          dataArray.push(
            _.map(queryResponse.aggregationResults, (aggregationResult) => {
              return aggregationResult.value;
            })
          );
        } else if (queryResponse.aggregationResults[0]?.groupByResult) {
          // Aggregation group by query
          // TODO - Revisit
          const columns = queryResponse.aggregationResults[0].groupByColumns;
          columns.push(queryResponse.aggregationResults[0].function);
          columnList = _.map(columns, (columnName) => {
            return columnName;
          });

          dataArray = _.map(
            queryResponse.aggregationResults[0].groupByResult,
            (aggregationGroup) => {
              const row = aggregationGroup.group;
              row.push(aggregationGroup.value);
              return row;
            }
          );
        }
      }
    } else if (queryResponse.resultTable?.dataSchema?.columnNames?.length) {
      columnList = queryResponse.resultTable.dataSchema.columnNames;
      dataArray = queryResponse.resultTable.rows;
    }

    const columnStats = [ 'timeUsedMs',
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
      'minConsumingFreshnessTimeMs'];

    return {
      result: {
        columns: columnList,
        records: dataArray,
      },
      queryStats: {
        columns: columnStats,
        records: [[data.timeUsedMs, data.numDocsScanned, data.totalDocs, data.numServersQueried, data.numServersResponded,
          data.numSegmentsQueried, data.numSegmentsProcessed, data.numSegmentsMatched, data.numConsumingSegmentsQueried,
          data.numEntriesScannedInFilter, data.numEntriesScannedPostFilter, data.numGroupsLimitReached,
          data.partialResponse ? data.partialResponse : '-', data.minConsumingFreshnessTimeMs]]
      },
      data,
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
  const columnHeaders = [
    'Table Name',
    'Reported Size',
    'Estimated Size',
    'Number of Segments',
    'Status',
  ];
  return getTenantTable(tenantName).then(({ data }) => {
    const tableArr = data.tables.map((table) => table);
    if (tableArr.length) {
      const promiseArr = [];
      tableArr.map((name) => {
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
              reportedSizeInBytes,
              estimatedSizeInBytes
            );
          } else if (index % 3 === 1) {
            // response of getIdealState API
            idealStateObj = result.data.OFFLINE || result.data.REALTIME;
          } else if (index % 3 === 2) {
            // response of getExternalView API
            externalViewObj = result.data.OFFLINE || result.data.REALTIME;
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
          columns: columnHeaders,
          records: finalRecordsArr,
        };
      });
    }
    return {
      columns: columnHeaders,
      records: []
    };
  });
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
// Expected Output: {columns: [], records: []}
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
          _.isEqual(idealStateObj[key], externalViewObj[key]) ? 'Good' : 'Bad',
        ];
      }),
    };
  });
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

// This method is used to display summary of a particular segment, replia set as well as JSON format of a tenant table
// API: /tables/tableName/externalview
//      /segments/:tableName/:segmentName/metadata
// Expected Output: {columns: [], records: []}
const getSegmentDetails = (tableName, segmentName) => {
  const promiseArr = [];
  promiseArr.push(getExternalView(tableName));
  promiseArr.push(getSegmentMetadata(tableName, segmentName));

  return Promise.all(promiseArr).then((results) => {
    const obj = results[0].data.OFFLINE || results[0].data.REALTIME;
    const segmentMetaData = results[1].data;

    const result = [];
    for (const prop in obj[segmentName]) {
      if (obj[segmentName]) {
        result.push([prop, obj[segmentName][prop]]);
      }
    }

    return {
      replicaSet: {
        columns: ['Server Name', 'Status'],
        records: [...result],
      },
      summary: {
        segmentName,
        totalDocs: segmentMetaData['segment.total.docs'],
        createTime: moment(+segmentMetaData['segment.creation.time']).format(
          'MMMM Do YYYY, h:mm:ss'
        ),
      },
      JSON: segmentMetaData
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
    return response.data;
  });
};

const getServerOfTenant = (tenantName) => {
  return getServerListOfTenant(tenantName).then((response)=>{
    return response.data.ServerInstances;
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

const deleteInstance = (instanceName) => {
  return dropInstance(instanceName).then((response)=>{
    return response.data;
  });
};

const reloadSegmentOp = (tableName, segmentName) => {
  return reloadSegment(tableName, segmentName).then((response)=>{
    return response.data;
  });
};

const deleteSegmentOp = (tableName, segmentName) => {
  return deleteSegment(tableName, segmentName).then((response)=>{
    return response.data;
  });
};

export default {
  getTenantsData,
  getAllInstances,
  getInstanceData,
  getClusterConfigData,
  getQueryTablesList,
  getTableSchemaData,
  getQueryResults,
  getTenantTableData,
  getTableSummaryData,
  getSegmentList,
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
  deleteInstance,
  deleteSegmentOp,
  reloadSegmentOp
};
