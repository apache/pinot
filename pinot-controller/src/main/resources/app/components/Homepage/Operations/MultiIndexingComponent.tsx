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
import { createStyles, FormControl, Grid, Input, InputLabel, makeStyles, MenuItem, Select, TextField, Theme, IconButton, Fab, Button, Chip, Tooltip} from '@material-ui/core';
import AddCircleIcon from '@material-ui/icons/AddCircle';
import ClearIcon from '@material-ui/icons/Clear';

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    formControl: {
      margin: theme.spacing(1),
      width: '100%'
    },
    selectFormControl: {
      margin: theme.spacing(1),
      width: 170
    },
    deleteIcon:{
        marginTop:15,
        color: theme.palette.error.main
    },
    autoCompleteControl: {
      '& .MuiFormControl-marginNormal': {
        marginTop: 0
      }
    },
    redColor: {
      color: theme.palette.error.main
    }
  })
);

type Props = {
    changeHandler: any,
    streamConfigsObj: Object,
    columnName:Array<string>,
    textDataObj: Array<TextObj>,
    tableDataObj:any
};

type TextObj = {
    name:string,
    encodingType:string,
    indexType:string
}

export default function MultiIndexingComponent({
    changeHandler,
    streamConfigsObj,
    columnName,
    tableDataObj,
    textDataObj
}: Props) {
    const classes = useStyles();
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

      const [jsonUpdated,setJsonUpdated] = useState(false);

    const updateFieldForColumnName = (tempStreamConfigObj,values,Field,majorField) =>{
        values.map((v)=>{
            let valChanged = false;
            tempStreamConfigObj.map((o)=>{
                if(v === o.columnName){
                    if("Encoding" === majorField)
                    {
                        o[majorField] = Field;
                    }else{
                        o[majorField].push(Field);
                    }
                    valChanged = true;
                }
            })
            if(!valChanged){
                tempStreamConfigObj.push({
                    columnName : v,
                    Indexing : [],
                    Encoding: "",
                    [majorField] : "Encoding" === majorField ? Field : [Field]
                })
            }
        })
        return tempStreamConfigObj;
    }

    const convertInputToData = (input,isJsonUpdated) =>{
            let tempStreamConfigObj = [];
            Object.keys(input).map((o)=>{
                switch(o){
                    case "invertedIndexColumns":
                        tempStreamConfigObj = updateFieldForColumnName(tempStreamConfigObj,input[o],"Inverted","Indexing");
                    break;
                    case "rangeIndexColumns":
                        tempStreamConfigObj = updateFieldForColumnName(tempStreamConfigObj,input[o],"Range","Indexing");
                    break;
                    case "sortedColumn":
                        tempStreamConfigObj = updateFieldForColumnName(tempStreamConfigObj,input[o],"Sorted","Indexing");
                    break;
                    case "bloomFilterColumns":
                        tempStreamConfigObj = updateFieldForColumnName(tempStreamConfigObj,input[o],"bloomFilter","Indexing");
                    break;
                    case "noDictionaryColumns":
                        tempStreamConfigObj = updateFieldForColumnName(tempStreamConfigObj,input[o],"None","Encoding");
                    break;
                    case "DictionaryColumns":
                        tempStreamConfigObj = updateFieldForColumnName(tempStreamConfigObj,input[o],"Dictionary","Encoding");
                    break;
                    case "onHeapDictionaryColumns":
                        tempStreamConfigObj = updateFieldForColumnName(tempStreamConfigObj,input[o],"heapDictionary","Encoding");
                    break;
                    case "varLengthDictionaryColumns":
                        tempStreamConfigObj = updateFieldForColumnName(tempStreamConfigObj,input[o],"varDictionary","Encoding");
                    break;
                }
            })
            if(textDataObj && textDataObj.length){
                textDataObj.map((o)=>{
                    tempStreamConfigObj = updateFieldForColumnName(tempStreamConfigObj,[o.name],"Text","Indexing");
                })
            }
            return tempStreamConfigObj;
    }

    const [streamConfigObj, setStreamConfigObj] = useState(convertInputToData(streamConfigsObj,false));

    const addButtonClick = ()=>{
        let data = {
            columnName : "",
            Indexing : [],
            Encoding: ""
        };
        let tempStreamConfigObj = [...streamConfigObj,data]
        setStreamConfigObj(tempStreamConfigObj);
        // changeHandler('tableIndexConfig',convertDataToInput(tempStreamConfigObj));
    }

    const convertDataToInput = async (input) => {
        let outputData = {
            invertedIndexColumns: [],
            rangeIndexColumns : [],
            sortedColumn : [],
            noDictionaryColumns : [],
            DictionaryColumns: [],
            onHeapDictionaryColumns : [],
            varLengthDictionaryColumns :  [],
            bloomFilterColumns : []
        };
        let fieldConfigList= [];

        input.map((i)=>{
            i.Indexing.map((o)=>{
                switch(o){
                    case "Sorted":
                        outputData.sortedColumn.push(i.columnName);
                    break;
                    case "Inverted":
                        outputData.invertedIndexColumns.push(i.columnName);
                    break;
                    case "Range":
                        outputData.rangeIndexColumns.push(i.columnName);
                    break;
                    case "bloomFilter":
                        outputData.bloomFilterColumns.push(i.columnName);
                    break;
                    case "Text":
                        fieldConfigList.push({"name":i.columnName, "encodingType":"RAW", "indexType":"TEXT"})
                    break;
                }
            })
                switch(i.Encoding){
                    case "None":
                        outputData.noDictionaryColumns.push(i.columnName);
                    break;
                    case "Dictionary":
                        outputData.DictionaryColumns.push(i.columnName);
                    break;
                    case "heapDictionary":
                        outputData.onHeapDictionaryColumns.push(i.columnName);
                    break;
                    case "varDictionary":
                        outputData.varLengthDictionaryColumns.push(i.columnName);
                    break;
                }
        });
        return {
            tableIndexConfig:outputData,
            fieldConfigList
        };
    }

    const keyChange = (input,index, value) =>{
        let tempStreamConfigObj = [...streamConfigObj];
        tempStreamConfigObj[index][input] = value;
        setStreamConfigObj(tempStreamConfigObj);
    }

    const updateJson = async () =>{
        changeHandler('tableIndexConfig',await convertDataToInput(streamConfigObj));
    }

    const deleteClick = async (ind) =>{
        let tempStreamConfigObj = [...streamConfigObj];
        tempStreamConfigObj.splice(ind,1);
        setStreamConfigObj(tempStreamConfigObj);
        changeHandler('tableIndexConfig',await convertDataToInput(tempStreamConfigObj));
    }

    useEffect(() => {
        if(jsonUpdated){
            setJsonUpdated(false);
        }
        else{
            let value = convertInputToData(streamConfigsObj,true);
            if(value.length >= streamConfigObj.length){
                setStreamConfigObj(value);
            }
            setJsonUpdated(true);
        }
    }, [streamConfigsObj]);

  return (
    <Grid container spacing={2}>
                {
                    streamConfigObj && streamConfigObj.map((o,i)=>{
                        return(
                            <Grid item xs={6}>
                                <div className="box-border">
                                <Grid container spacing={2}>
                                    <Grid item xs={3}>
                                        <FormControl className={classes.formControl}>
                                            <InputLabel htmlFor={o.columnName}>Column Name</InputLabel>
                                            <Select
                                                labelId={o.columnName}
                                                id={o.columnName}
                                                value={o.columnName}
                                                key={i+"columnName"}
                                                onChange={(e)=> keyChange("columnName",i,e.target.value)}
                                                onBlur={updateJson}
                                            >
                                               {columnName.map((val)=>{
                                                      return <MenuItem value={val}>{val}</MenuItem>
                                                    })}
                                            </Select>
                                        </FormControl>
                                    </Grid>
                                    <Grid item xs={4}>
                                    <Tooltip title="Select a column encoding. By default, all columns are dictionary encoded." arrow placement="top-start">
                                        <FormControl className={classes.formControl}>
                                        <InputLabel htmlFor={o.Encoding}>Encoding</InputLabel>
                                             <Select
                                                labelId={o.Encoding}
                                                id={o.Encoding}
                                                key={i+"Encoding"}
                                                value={o.Encoding}
                                                onChange={(e)=> keyChange("Encoding",i,e.target.value)}
                                                onBlur={updateJson}
                                            >
                                                <MenuItem value="None">None</MenuItem>
                                                <MenuItem value="Dictionary">Dictionary</MenuItem>
                                                <MenuItem value="varDictionary">Variable Length Dictionary</MenuItem>
                                                <MenuItem value="heapDictionary">On Heap Dictionary</MenuItem>
                                            </Select>
                                        </FormControl>
                                    </Tooltip>
                                    </Grid>
                                    <Grid item xs={4}>
                                    <Tooltip title="Select indexes to apply. By default, no indexing is applied." arrow placement="top-start">
                                        <FormControl className={classes.formControl}>
                                        <InputLabel htmlFor={i+"keymulti"}>Indexing</InputLabel>
                                                <Select
                                                    labelId={i+"keymulti"}
                                                    id={i+"keymulti"}
                                                    key={i+"Indexing"}
                                                    multiple
                                                    value={o.Indexing || []}
                                                    onChange={(e)=> keyChange("Indexing",i,e.target.value)}
                                                    input={<Input id="select-multiple-chip" />}
                                                    renderValue={(selected) => (
                                                        <div className={"chips"}>
                                                        {(selected as string[]).map((value) => (
                                                            <Chip key={value} label={value} className={"chip"} />
                                                        ))}
                                                        </div>
                                                    )}
                                                    MenuProps={MenuProps}
                                                    onBlur={updateJson}>
                                                <MenuItem value="None">None</MenuItem>
                                                <MenuItem value="Sorted">Sorted</MenuItem>
                                                <MenuItem value="Inverted">Inverted</MenuItem>
                                                <MenuItem value="Range">Range</MenuItem>
                                                <MenuItem value="Text">Text</MenuItem>
                                                <MenuItem value="bloomFilter">Bloom filter</MenuItem>
                                            </Select>
                                        </FormControl>
                                    </Tooltip>
                                    </Grid>
                                    <Grid item xs={1}>
                                        <FormControl>
                                            <IconButton aria-label="delete" className={classes.deleteIcon} onClick={()=>{
                                                deleteClick(i)}}>
                                                <ClearIcon />
                                            </IconButton>
                                        </FormControl>
                                    </Grid>
                                </Grid>
                            </div>
                            </Grid>)
                        })
                }
                <Grid item xs={3}>
                <FormControl className={classes.formControl}>
                    <Button
                    aria-label="plus"
                    variant="outlined"
                    color="primary"
                    onClick={addButtonClick}
                    startIcon={(<AddCircleIcon />)}
                    >
                        Add new Field
                    </Button>
                </FormControl>
                </Grid>
    </Grid>
  );
}