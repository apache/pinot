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

import React, { useEffect, useState } from 'react';
import { createStyles, DialogContent, Grid, makeStyles, Theme, Button, ButtonGroup } from '@material-ui/core';
import Dialog from '../../CustomDialog';
import SimpleAccordion from '../../SimpleAccordion';
import AddTableComponent from './AddTableComponent';
import CustomCodemirror from '../../CustomCodemirror';
import PinotMethodUtils from '../../../utils/PinotMethodUtils';
import { NotificationContext } from '../../Notification/NotificationContext';
import AddTenantComponent from './AddTenantComponent';
import AddIngestionComponent from './AddIngestionComponent';
import AddIndexingComponent from './AddIndexingComponent';
import AddPartionComponent from './AddPartionComponent';
import AddStorageComponent from './AddStorageComponent';
import AddQueryComponent from './AddQueryComponent';
import { isEmpty, isArray } from 'lodash';
import AddRealTimeIngestionComponent from './AddRealTimeIngestionComponent';
import AddRealTimePartionComponent from './AddRealTimePartionComponent';

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    sqlDiv: {
      border: '1px #BDCCD9 solid',
      borderRadius: 4,
      marginBottom: '20px',
    },
    queryOutput: {
      '& .CodeMirror': { height: '532px !important' },
    },
  })
);
// View modes for simple form or raw JSON editing
enum EditView {
  SIMPLE = "SIMPLE",
  JSON = "JSON"
}

type Props = {
  hideModal: (event: React.MouseEvent<HTMLElement, MouseEvent>) => void,
  fetchData: Function,
  tableType: String
};

const defaultTableObj = {
  "tableName": "",
  "tableType": "",
  "tenants": {
    "broker": "DefaultTenant",
    "server": "DefaultTenant",
    "tagOverrideConfig": {}
  },
  "segmentsConfig": {
    "schemaName": "",
    "timeColumnName": null,
    "replication": "1",
    "replicasPerPartition": "1",
    "retentionTimeUnit": null,
    "retentionTimeValue": null,
    "completionConfig": null,
    "crypterClassName": null,
    "peerSegmentDownloadScheme": null
  },
  "tableIndexConfig": {
    "loadMode": "MMAP",
    "invertedIndexColumns": [],
    "createInvertedIndexDuringSegmentGeneration": false,
    "rangeIndexColumns": [],
    "sortedColumn": [],
    "bloomFilterColumns": [],
    "bloomFilterConfigs": null,
    "noDictionaryColumns": [],
    "onHeapDictionaryColumns": [],
    "varLengthDictionaryColumns": [],
    "enableDefaultStarTree": false,
    "starTreeIndexConfigs": null,
    "enableDynamicStarTreeCreation": false,
    "segmentPartitionConfig": null,
    "columnMinMaxValueGeneratorMode": null,
    "aggregateMetrics": false,
    "nullHandlingEnabled": false,
    "streamConfigs": {
      "streamType": "kafka",
      "stream.kafka.topic.name": "",
      "stream.kafka.broker.list": "",
      "stream.kafka.consumer.prop.auto.offset.reset": "smallest",
      "stream.kafka.consumer.factory.class.name": "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory",
      "stream.kafka.decoder.class.name": "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder",
      "realtime.segment.flush.threshold.rows": "0",
      "realtime.segment.flush.threshold.segment.rows": "0",
      "realtime.segment.flush.threshold.time": "24h",
      "realtime.segment.flush.threshold.segment.size": "100M"
    }
  },
  "metadata": {},
  "ingestionConfig": {
    "filterConfig": null,
    "transformConfigs": null
  },
  "quota": {
    "storage": null,
    "maxQueriesPerSecond": null
  },
  "task": null,
  "routing": {
    "segmentPrunerTypes": null,
    "instanceSelectorType": null
  },
  "query": {
    "timeoutMs": null
  },
  "fieldConfigList": null,
  "upsertConfig": null,
  "tierConfigs": null
};

const defaultSchemaObj = {
  schemaName: '',
  dimensionFieldSpecs: [],
  metricFieldSpecs: [],
  dateTimeFieldSpecs: []
};

let timerId = null;

const tableNamekey = ["dimensionFieldSpecs","metricFieldSpecs","dateTimeFieldSpecs"];

export default function AddRealtimeTableOp({
  hideModal,
  fetchData,
  tableType
}: Props) {
  const classes = useStyles();
  const [editView, setEditView] = useState<EditView>(EditView.SIMPLE);
  const [jsonTableObj, setJsonTableObj] = useState(JSON.parse(JSON.stringify(defaultTableObj)));
  const [tableObj, setTableObj] = useState(JSON.parse(JSON.stringify(defaultTableObj)));
  const [schemaObj, setSchemaObj] = useState(JSON.parse(JSON.stringify(defaultSchemaObj)));
  const [tableName, setTableName] = useState('');
  const [columnName, setColumnName] = useState([]);
  const {dispatch} = React.useContext(NotificationContext);
  let isError = false;

  useEffect(()=>{
    if(tableName !== tableObj.tableName){
      setTableName(tableObj.tableName);
      clearTimeout(timerId);
      timerId = setTimeout(()=>{
        updateSchemaObj(tableObj.tableName);
      }, 1000);
    }
  }, [tableObj]);

  useEffect(()=>{
    setTableObj({...tableObj,"tableType":tableType})
  },[])
  // Sync state when toggling between simple and JSON view
  useEffect(() => {
    if (editView === EditView.JSON) {
      setJsonTableObj(JSON.parse(JSON.stringify(tableObj)));
    } else {
      setTableObj(JSON.parse(JSON.stringify(jsonTableObj)));
    }
  }, [editView]);

  const updateSchemaObj = async (tableName) => {
    //table name is same as schema name
    const schemaObj = await PinotMethodUtils.getSchemaData(tableName);
    if(schemaObj.error || typeof schemaObj === 'string'){
      dispatch({
        type: 'error',
        message: schemaObj.error || schemaObj,
        show: true
      });
      setSchemaObj(defaultSchemaObj)
    } else {
      setSchemaObj({...defaultSchemaObj, ...schemaObj});
    }
  }

  const returnValue = (data,key) =>{
    Object.keys(data).map(async (o)=>{
    if(!isEmpty(data[o]) && typeof data[o] === "object"){
        await returnValue(data[o],key);
      }
      else if(!isEmpty(data[o]) && isArray(data[o])){
        data[o].map(async (obj)=>{
          await returnValue(obj,key);
        })
     }else{
        if(o === key && (data[key] === null || data[key] === "")){
          dispatch({
            type: 'error',
            message: `${key} cannot be empty`,
            show: true
          });
          isError = true;
        }
      }
    })
  }

const checkFields = (tableObj,fields) => {
    fields.forEach(async (o:any)=>{
        if(tableObj[o.key] === undefined){
          await returnValue(tableObj,o.key);
        }else{
          if((tableObj[o.key] === null || tableObj[o.key] === "")){
            dispatch({
              type: 'error',
              message: `${o.label} cannot be empty`,
              show: true
            });
            isError = true;
          }
        }
    });
  }

  const validateTableConfig = async () => {
    const fields = [{key:"tableName",label:"Table Name"},{key:"tableType",label:"Table Type"},{key:"stream.kafka.broker.list",label:"stream.kafka.broker.list"},{key:"stream.kafka.topic.name",label:"stream.kafka.topic.name"},{key:"stream.kafka.decoder.class.name",label:"stream.kafka.decoder.class.name"}];
    await checkFields(tableObj,fields);
    if(isError){
      isError  = false;
      return false;
    }
    const validTable = await PinotMethodUtils.validateTableAction(tableObj);
    if(validTable.error || typeof validTable === 'string'){
      dispatch({
        type: 'error',
        message: validTable.error || validTable,
        show: true
      });
      return false;
    }
    return true;
  };

  const handleSave = async () => {
    // Determine which config to save based on view
    const configToSave = editView === EditView.SIMPLE ? tableObj : jsonTableObj;
    // Validate based on view
    if (editView === EditView.SIMPLE) {
      if (!await validateTableConfig()) {
        return;
      }
    } else {
      const validTable = await PinotMethodUtils.validateTableAction(configToSave);
      if (validTable.error || typeof validTable === 'string') {
        dispatch({
          type: 'error',
          message: validTable.error || validTable,
          show: true
        });
        return;
      }
    }
    const tableCreationResp = await PinotMethodUtils.saveTableAction(configToSave);
    dispatch({
      type: (tableCreationResp.error || typeof tableCreationResp === 'string') ? 'error' : 'success',
      message: tableCreationResp.error || tableCreationResp.status || tableCreationResp,
      show: true
    });
    if (tableCreationResp.status) {
      fetchData();
      hideModal(null);
    }
  };

  useEffect(()=>{
    let columnName = [];
    if(!isEmpty(schemaObj)){
      tableNamekey.map((o)=>{
        schemaObj[o] && schemaObj[o].map((obj)=>{
          columnName.push(obj.name);
        })
      })
    }
    setColumnName(columnName);
  },[schemaObj])

  return (
    <Dialog
      open={true}
      handleClose={hideModal}
      handleSave={handleSave}
      title={
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <span>Add {tableType} Table</span>
          <ButtonGroup size="small" color="primary">
            <Button
              variant={editView === EditView.SIMPLE ? 'contained' : 'outlined'}
              onClick={() => setEditView(EditView.SIMPLE)}
            >
              Simple
            </Button>
            <Button
              variant={editView === EditView.JSON ? 'contained' : 'outlined'}
              onClick={() => setEditView(EditView.JSON)}
            >
              Json
            </Button>
          </ButtonGroup>
        </div>
      }
      size="xl"
      disableBackdropClick={true}
      disableEscapeKeyDown={true}
    >
      <DialogContent>
        {editView === EditView.SIMPLE && (
          <Grid container spacing={2}>
          <Grid item xs={12}>
            <SimpleAccordion
              headerTitle="Add Table"
              showSearchBox={false}
            >
              <AddTableComponent
                tableObj={tableObj}
                setTableObj={setTableObj}
                dateTimeFieldSpecs={schemaObj.dateTimeFieldSpecs}
                disable={tableType !== ""}
              />
            </SimpleAccordion>
          </Grid>
          <Grid item xs={12}>
            <SimpleAccordion
              headerTitle="Tenants"
              showSearchBox={false}
            >
              <AddTenantComponent
                tableObj={{...tableObj}}
                setTableObj={setTableObj}
              />
            </SimpleAccordion>
          </Grid>
          <Grid item xs={12}>
            <SimpleAccordion
              headerTitle="Ingestion"
              showSearchBox={false}
            >
              <AddRealTimeIngestionComponent
                tableObj={{...tableObj}}
                setTableObj={setTableObj}
                columnName={columnName}
              />
            </SimpleAccordion>
          </Grid>
          <Grid item xs={12}>
            <SimpleAccordion
              headerTitle="Indexing & encoding"
              tooltipContent={<a className = {"tooltip-link"} target="_blank" href={"https://docs.pinot.apache.org/basics/indexing"}>Click here for more details</a>}
              showSearchBox={false}
            >
              <AddIndexingComponent
                tableObj={tableObj}
                setTableObj={setTableObj}
                columnName={columnName}
              />
            </SimpleAccordion>
          </Grid>
          <Grid item xs={12}>
            <SimpleAccordion
              headerTitle="Partitioning & Routing"
              showSearchBox={false}
            >
              <AddRealTimePartionComponent
                tableObj={tableObj}
                setTableObj={setTableObj}
                columnName={columnName}
              />
            </SimpleAccordion>
          </Grid>
          <Grid item xs={12}>
            <SimpleAccordion
              headerTitle="Storage & Data retention"
              showSearchBox={false}
            >
              <AddStorageComponent
                tableObj={tableObj}
                setTableObj={setTableObj}
              />
            </SimpleAccordion>
          </Grid>
          <Grid item xs={12}>
            <SimpleAccordion
              headerTitle="Query"
              showSearchBox={false}
            >
              <AddQueryComponent
                tableObj={tableObj}
                setTableObj={setTableObj}
              />
            </SimpleAccordion>
          </Grid>
          <Grid item xs={6}>
            <div className={classes.sqlDiv}>
              <SimpleAccordion
                headerTitle="Table Config"
                showSearchBox={false}
              >
                <CustomCodemirror
                  customClass={classes.queryOutput}
                  data={tableObj}
                  isEditable={true}
                  returnCodemirrorValue={(newValue)=>{
                    try{
                      const jsonObj = JSON.parse(newValue);
                      if(jsonObj){
                        jsonObj.segmentsConfig.replicasPerPartition = jsonObj.segmentsConfig.replication;
                        setTableObj(jsonObj);
                      }
                    }catch(e){}
                  }}
                />
              </SimpleAccordion>
            </div>
          </Grid>
          <Grid item xs={6}>
            <div className={classes.sqlDiv}>
              <SimpleAccordion
                headerTitle="Schema Config (Read only)"
                showSearchBox={false}
              >
                <CustomCodemirror
                  customClass={classes.queryOutput}
                  data={schemaObj}
                  isEditable={false}
                />
              </SimpleAccordion>
            </div>
          </Grid>
        </Grid>
        )}
        {editView === EditView.JSON && (
          <CustomCodemirror
            data={jsonTableObj}
            isEditable={true}
            returnCodemirrorValue={(newValue) => {
              try {
                const jsonObj = JSON.parse(newValue);
                setJsonTableObj(jsonObj);
              } catch (e) {}
            }}
          />
        )}
      </DialogContent>
    </Dialog>
  );
}