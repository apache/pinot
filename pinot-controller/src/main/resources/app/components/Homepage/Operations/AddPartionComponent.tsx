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
import { createStyles, FormControl, Grid, Input, InputLabel, makeStyles, MenuItem, Select, Theme, Chip} from '@material-ui/core';

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    formControl: {
      margin: theme.spacing(1),
      minWidth: 120,
      width: 300
    },
    selectFormControl: {
      margin: theme.spacing(1),
      width: 300
    }
  })
);

type Props = {
  tableObj: any,
  setTableObj: Function,
  columnName: Array<string>
};

export default function AddPartionComponent({
  tableObj,
  setTableObj,
  columnName
}: Props) {
  const classes = useStyles();

  const [tableDataObj, setTableDataObj] = useState(tableObj);
  const [showPartition, setShowPartition] = useState(0);
  const [showReplica, setShowReplica] = useState(0);
  const ITEM_HEIGHT = 48;
  const ITEM_PADDING_TOP = 8;
  const MenuProps = {
      PaperProps: {
        style: {
          maxHeight: ITEM_HEIGHT * 4.5 + ITEM_PADDING_TOP,
          width: 250,
        },
      },
    };

  const changeHandler = (fieldName, value) => {
    let newTableObj = {...tableDataObj};
    switch(fieldName){
      case 'segmentPrunerTypes':
        // newTableObj[fieldName] = value;
        setShowPartition(value);
        if(value){
            newTableObj.routing.segmentPrunerTypes = ["partition"];
            newTableObj.tableIndexConfig.segmentPartitionConfig = {
                columnPartitionMap : {
                    memberId : {
                        functionName : ["Murmur"]
                    }
                }
            }
        }else{
            delete newTableObj.routing.segmentPrunerTypes;
            newTableObj.tableIndexConfig.segmentPartitionConfig = null;
        }
      break;
      case 'functionName':
        newTableObj.tableIndexConfig.segmentPartitionConfig.columnPartitionMap.memberId.functionName = value;
      break;
      case 'numPartitions':
        newTableObj.tableIndexConfig.segmentPartitionConfig.columnPartitionMap.memberId.numPartitions = value;
      break;
      case 'instanceSelectorType':
          if(value){
            newTableObj.routing.instanceSelectorType = "replicaGroup"
            newTableObj.instanceAssignmentConfigMap = {
                 ["OFFLINE"]: {
                     replicaGroupPartitionConfig: {
                         replicaGroupBased: true,
                         numReplicaGroups: null,
                         numInstancesPerReplicaGroup:null
                        }
                    }
                }
          }
          else{
            delete newTableObj.routing.instanceSelectorType;
            newTableObj.instanceAssignmentConfigMap = null;
          }
          setShowReplica(value);
        break;
        case 'numReplicaGroups':
            newTableObj.instanceAssignmentConfigMap["OFFLINE"].replicaGroupPartitionConfig.numReplicaGroups = value;
        break;
        case 'numInstancesPerReplicaGroup':
            newTableObj.instanceAssignmentConfigMap["OFFLINE"].replicaGroupPartitionConfig.numInstancesPerReplicaGroup = value;
        break;
        case 'columnName':
            newTableObj.tableIndexConfig.segmentPartitionConfig.columnPartitionMap.memberId.columnName = value;
        break;
    };
    setTableDataObj(newTableObj);
    setTableObj(newTableObj);
  };

  useEffect(()=>{
    setTableDataObj(tableObj);
  }, [tableObj]);

  return (
    <Grid container spacing={2}>
      <Grid item xs={12}>
      <FormControl className={classes.selectFormControl}>
          <InputLabel htmlFor="segmentPrunerTypes">Enable partitioning</InputLabel>
          <Select
            labelId="segmentPrunerTypes"
            id="segmentPrunerTypes"
            value={showPartition}
            onChange={(e)=> changeHandler('segmentPrunerTypes', e.target.value)}
          >
            <MenuItem value={1}>True</MenuItem>
            <MenuItem value={0}>False</MenuItem>
          </Select>
        </FormControl>
         {
            tableDataObj.routing.segmentPrunerTypes ?
                <FormControl className={classes.selectFormControl}>
                    <InputLabel htmlFor="columnName">Column Name</InputLabel>
                        <Select
                            labelId="columnName"
                            id="columnName"
                            key="columnName"
                            value={tableDataObj.tableIndexConfig.segmentPartitionConfig.columnPartitionMap.memberId.columnName || ""}
                            onChange={(e)=> changeHandler('columnName', e.target.value)}
                            >
                              {columnName.map((val)=>{
                                                      return <MenuItem value={val}>{val}</MenuItem>
                                                    })}
                    </Select>
                </FormControl> : null
         }
         {
            tableDataObj.routing.segmentPrunerTypes ?
                <FormControl className={classes.selectFormControl}>
                    <InputLabel htmlFor="functionName">Function Name</InputLabel>
                        <Select
                            labelId="functionNamePartition"
                            id="functionNamePartition"
                            key="functionName"
                            value={tableDataObj.tableIndexConfig.segmentPartitionConfig.columnPartitionMap.memberId.functionName}
                            onChange={(e)=> changeHandler('functionName', e.target.value)}
                           >

                        <MenuItem value="Modulo">Modulo</MenuItem>
                        <MenuItem value="Murmur">Murmur</MenuItem>
                        <MenuItem value="ByteArray">ByteArray</MenuItem>
                        <MenuItem value="HashCode">HashCode</MenuItem>
                    </Select>
                </FormControl> : null
         }
        {
            tableDataObj.routing.segmentPrunerTypes ?
                <FormControl className={classes.formControl} >
                    <InputLabel htmlFor="numPartitions">Number of partitions​</InputLabel>
                    <Input
                        id="numPartitions"
                        value={tableDataObj.tableIndexConfig.segmentPartitionConfig.columnPartitionMap.memberId.numPartitions}
                        onChange={(e)=> changeHandler('numPartitions', e.target.value)}
                        type="number"
                    />
                </FormControl> : null }
            </Grid>
        <Grid item xs={12}>
        <FormControl className={classes.selectFormControl}>
            <InputLabel htmlFor="instanceSelectorType">Enable replica groups</InputLabel>
            <Select
            labelId="instanceSelectorType"
            id="instanceSelectorType"
            value={showReplica}
            onChange={(e)=> changeHandler('instanceSelectorType', e.target.value)}
            >
                <MenuItem value={1}>True</MenuItem>
                <MenuItem value={0}>False</MenuItem>
            </Select>
        </FormControl>

        {
            tableDataObj.routing.instanceSelectorType ?
                <FormControl className={classes.formControl} >
                    <InputLabel htmlFor="numReplicaGroups">Number of replica groups</InputLabel>
                    <Input
                        id="numReplicaGroups"
                        value={tableDataObj.instanceAssignmentConfigMap ? Number(tableDataObj.instanceAssignmentConfigMap["OFFLINE"].replicaGroupPartitionConfig.numReplicaGroups) : null}
                        onChange={(e)=> changeHandler('numReplicaGroups', e.target.value)}
                        type="number"
                    />
                </FormControl>
            : null
        }

        {
            tableDataObj.routing.instanceSelectorType ?
                <FormControl className={classes.formControl} >
                    <InputLabel htmlFor="numInstancesPerReplicaGroup">Number of instances per replica group​</InputLabel>
                    <Input
                        id="numInstancesPerReplicaGroup"
                        value={tableDataObj.instanceAssignmentConfigMap ? Number(tableDataObj.instanceAssignmentConfigMap["OFFLINE"].replicaGroupPartitionConfig.numInstancesPerReplicaGroup) : null}
                        onChange={(e)=> changeHandler('numInstancesPerReplicaGroup', e.target.value)}
                        type="number"
                    />
                </FormControl>
            : null
        }
      </Grid>
    </Grid>
  );
}