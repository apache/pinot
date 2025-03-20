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
import { createStyles, FormControl, Grid, Input, InputLabel, makeStyles, MenuItem, Select, Theme, Tooltip} from '@material-ui/core';
import AddDeleteComponent from './AddDeleteComponent';
import MultipleSelectComponent from './MultipleSelectComponent';
import { isEmpty } from 'lodash';

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    formControl: {
      margin: theme.spacing(1),
      minWidth: 120,
    },
    selectFormControl: {
      margin: theme.spacing(1),
      width: 220
    },
  })
);

type Props = {
  tableObj: any,
  setTableObj: Function,
  columnName: Array<string>
};

export default function AddIngestionComponent({
  tableObj,
  setTableObj,
  columnName
}: Props) {
  const classes = useStyles();
  const [newSize,setNewSize] = useState([0,1])
  const [tableDataObj, setTableDataObj] = useState(tableObj);

  const changeHandler = (fieldName, value) => {
    let newTableObj = {...tableDataObj};
    switch(fieldName){
      case 'filterConfig':
        newTableObj.ingestionConfig.filterConfig[fieldName] = value;
      break;
      case 'segmentPushFrequency':
        newTableObj.segmentsConfig[fieldName] = value;
      break;
      case 'segmentPushType':
        newTableObj.segmentsConfig[fieldName] = value;
      break;
      case 'streamConfigs':
        newTableObj.tableIndexConfig.streamConfigs = {...value};
      break;
      case 'filterFunction':
          if(!newTableObj.ingestionConfig.filterConfig){
            newTableObj.ingestionConfig.filterConfig = {};
          }
        newTableObj.ingestionConfig.filterConfig.filterFunction = value;
      break;
      case 'transformConfigs':
        tableDataObj.ingestionConfig.transformConfigs = value;
    };
    setTableDataObj(newTableObj);
    setTableObj(newTableObj);
  };

  useEffect(()=>{
    let newTableObj = {...tableObj};
      if(newTableObj.tableType === "REALTIME" && !newTableObj.streamConfigs && isEmpty(newTableObj.streamConfigs) ){
        newTableObj.tableIndexConfig.streamConfigs =
        {
            "streamType": "kafka",
            "stream.kafka.topic.name": "",
            "stream.kafka.broker.list": "",
            "stream.kafka.consumer.prop.auto.offset.reset": "smallest",
            "stream.kafka.consumer.factory.class.name":"org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory",
            "stream.kafka.decoder.class.name":"org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder",
            "realtime.segment.flush.threshold.rows": "0",
            "realtime.segment.flush.threshold.segment.rows": "0",
            "realtime.segment.flush.threshold.time": "24h",
            "realtime.segment.flush.threshold.segment.size": "100M"
        }
        setTableObj(newTableObj);
      }else if(newTableObj.tableType !== "REALTIME" && newTableObj.streamConfigs){
        newTableObj.streamConfigs = null;
        setTableObj(newTableObj);
      }
    setTableDataObj(newTableObj);
  }, [tableObj]);

  useEffect(()=>{
    setNewSize(newSize);
  },[newSize])

  return (
    <Grid container spacing={2}>
        <Grid item xs={12}>
            {
                tableDataObj.tableType === "OFFLINE" ?
                    <FormControl className={classes.selectFormControl}>
                        <InputLabel htmlFor="segmentPushFrequency">Offline push frequency</InputLabel>
                        <Select
                            labelId="segmentPushFrequency"
                            id="segmentPushFrequency"
                            value={tableDataObj.segmentsConfig.segmentPushFrequency !== "" ? tableDataObj.segmentsConfig.segmentPushFrequency : ""}
                            onChange={(e)=> changeHandler('segmentPushFrequency', e.target.value)}
                        >
                            <MenuItem value="HOURLY">HOURLY</MenuItem>
                            <MenuItem value="DAILY">DAILY</MenuItem>
                        </Select>
                    </FormControl> : null
            }
            {
                tableDataObj.tableType === "OFFLINE" ?
                    <FormControl className={classes.selectFormControl}>
                        <InputLabel htmlFor="segmentPushType">Offline push type</InputLabel>
                        <Select
                            labelId="segmentPushType"
                            id="segmentPushType"
                            value={tableDataObj.segmentsConfig.segmentPushType !== "" ? tableDataObj.segmentsConfig.segmentPushType : ""}
                            onChange={(e)=> changeHandler('segmentPushType', e.target.value)}
                        >
                            <MenuItem value="APPEND">APPEND</MenuItem>
                            <MenuItem value="REFRESH">REFRESH</MenuItem>
                        </Select>
                    </FormControl> : null
            }
            </Grid>
            <Grid item xs={12}>
            <Tooltip interactive title={(<>Filter out rows which match condition.<a target="_blank" href="https://docs.pinot.apache.org/developers/advanced/ingestion-level-transformations#filtering" className={"tooltip-link"}>(Click here for more details)</a></>)} arrow placement="top-start">
            <FormControl className={classes.formControl}>
                <InputLabel htmlFor="filterFunction">Filter function</InputLabel>
                <Input
                    id="filterFunction"
                    value={tableObj.ingestionConfig.filterConfig && tableObj.ingestionConfig.filterConfig.filterFunction || ""}
                    onChange={(e)=> changeHandler('filterFunction', e.target.value)}
                />
            </FormControl>
            </Tooltip>
            {
                tableDataObj.tableIndexConfig.streamConfigs ?
                <AddDeleteComponent
                    key = {"streamConfigs"}
                    streamConfigsObj = {{...tableDataObj.tableIndexConfig.streamConfigs}}
                    changeHandler = {changeHandler}/>
                : null
            }
            <Tooltip interactive title={(<>Transform the data values using Groovy or other inbuilt functions.<a target="_blank" href="https://docs.pinot.apache.org/developers/advanced/ingestion-level-transformations#column-transformation" className={"tooltip-link"}>(Click here for more details)</a></>)} arrow placement="top-start">
            <MultipleSelectComponent
                key = {"transformConfigs"}
                streamConfigsObj = {tableDataObj.ingestionConfig.transformConfigs || []}
                changeHandler = {changeHandler}
                columnName= {columnName}/>
            </Tooltip>
          </Grid>
    </Grid>
  );
}