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
import { createStyles, Grid, makeStyles, Theme} from '@material-ui/core';
import MultiIndexingComponent from './MultiIndexingComponent';

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    formControl: {
      margin: theme.spacing(1),
      minWidth: 120,
    },
    selectFormControl: {
      margin: theme.spacing(1),
      width: 220
    }
  })
);

type Props = {
  tableObj: any,
  setTableObj: Function,
  columnName: Array<string>
};

export default function AddIndexingComponent({
  tableObj,
  setTableObj,
  columnName
}: Props) {
  const classes = useStyles();

  const [tableDataObj, setTableDataObj] = useState(tableObj);
  const [showTree,setShowTree] = useState(0);
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
      case 'maxLeafRecords':
        newTableObj.tableIndexConfig.starTreeIndexConfigs[0].maxLeafRecords = value;
        // newTableObj.segmentsConfig.schemaName = value;
      break;
      case 'enableStarTree':
        setShowTree(value);
      break;
      case 'dimensionsSplitOrder':
          if(!newTableObj.tableIndexConfig.starTreeIndexConfigs){
            newTableObj.tableIndexConfig.starTreeIndexConfigs = [];
            newTableObj.tableIndexConfig.starTreeIndexConfigs[0] = {};
          }
          newTableObj.tableIndexConfig.starTreeIndexConfigs[0].dimensionsSplitOrder = value;
      break;
      case 'maxLeafRecords':
        if(!newTableObj.tableIndexConfig.starTreeIndexConfigs){
            newTableObj.tableIndexConfig.starTreeIndexConfigs = [];
            newTableObj.tableIndexConfig.starTreeIndexConfigs[0] = {};
          }
        newTableObj.tableIndexConfig.starTreeIndexConfigs[0].maxLeafRecords = value;
      break;
      case 'tableIndexConfig':
        newTableObj.tableIndexConfig  = {...newTableObj.tableIndexConfig,...value.tableIndexConfig};
        newTableObj.fieldConfigList = [...value.fieldConfigList];
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
      {/* <FormControl className={classes.selectFormControl}>
            <InputLabel htmlFor="enableStarTree">Enable default star tree?</InputLabel>
            <Select
            labelId="enableStarTree"
            id="enableStarTree"
            value={showTree}
            onChange={(e)=> changeHandler('enableStarTree', e.target.value)}
            >
                <MenuItem value={1}>True</MenuItem>
                <MenuItem value={0}>False</MenuItem>
            </Select>
        </FormControl>
      <FormControl className={classes.selectFormControl}>
            <InputLabel htmlFor="dimensionsSplitOrder">Dimension split order</InputLabel>
            <Select
                labelId="dimensionsSplitOrder"
                id="dimensionsSplitOrder"
                key="dimensionsSplitOrder"
                multiple
                value={tableDataObj.tableIndexConfig.starTreeIndexConfigs && tableDataObj.tableIndexConfig.starTreeIndexConfigs[0].dimensionsSplitOrder || []}
                onChange={(e)=> changeHandler('dimensionsSplitOrder', e.target.value)}
                input={<Input id="select-multiple-chip" />}
                renderValue={(selected) => (
                    <div className={"chips"}>
                    {(selected as string[]).map((value) => (
                        <Chip key={value} label={value} className={"chip"} />
                    ))}
                    </div>
                )}
                MenuProps={MenuProps}
                >
                <MenuItem value="Country">Country</MenuItem>
                <MenuItem value="Browser">Browser</MenuItem>
                <MenuItem value="Locale">Locale</MenuItem>
            </Select>
        </FormControl>
        <FormControl className={classes.formControl} >
            <InputLabel htmlFor="maxLeafRecords">Max leaf records</InputLabel>
            <Input
                id="maxLeafRecords"
                value={tableDataObj.tableIndexConfig.starTreeIndexConfigs && tableDataObj.tableIndexConfig.starTreeIndexConfigs[0].maxLeafRecords}
                onChange={(e)=> changeHandler('maxLeafRecords', e.target.value)}
                error = {tableDataObj.tableIndexConfig.starTreeIndexConfigs && tableDataObj.tableIndexConfig.starTreeIndexConfigs[0].maxLeafRecords ? isNaN(Number(tableDataObj.tableIndexConfig.starTreeIndexConfigs[0].maxLeafRecords)) : false}
            />
        </FormControl> */}
        <MultiIndexingComponent
            key = {"multiIndex"}
            streamConfigsObj = {{...tableDataObj.tableIndexConfig}}
            textDataObj={tableDataObj.fieldConfigList ? [...tableDataObj.fieldConfigList] : []}
            changeHandler = {changeHandler}
            tableDataObj = {tableDataObj}
            columnName={columnName}/>
        {/* <MultiMetricComponent
            key={"multiMetrix"}
            streamConfigsObj={tableDataObj.tableIndexConfig.starTreeIndexConfigs ? {...tableDataObj.tableIndexConfig.starTreeIndexConfigs[0]} : {}}
            changeHandler = {changeHandler}/> */}
      </Grid>
    </Grid>
  );
}