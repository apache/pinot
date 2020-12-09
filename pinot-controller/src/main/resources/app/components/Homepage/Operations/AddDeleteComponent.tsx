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
import { createStyles, FormControl, Grid, Input, InputLabel, makeStyles, MenuItem, Select, TextField, Theme, IconButton, Fab, Button, Tooltip} from '@material-ui/core';
import AddIcon from '@material-ui/icons/Add';
import AddCircleIcon from '@material-ui/icons/AddCircle';
import ClearIcon from '@material-ui/icons/Clear';
import DeleteIcon from '@material-ui/icons/Delete';
import InfoIcon from '@material-ui/icons/Info';

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    formControl: {
      margin: theme.spacing(1),
      width: '100%'
    },
    deleteIcon:{
        marginTop:15,
        marginLeft:-15,
        color: theme.palette.error.main
    },
    selectFormControl: {
      margin: theme.spacing(1),
      width: 170
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
  streamConfigsObj: Object
};

const compulsoryKeys = ["stream.kafka.broker.list","stream.kafka.topic.name","stream.kafka.consumer.type","stream.kafka.decoder.class.name"];

export default function AddDeleteComponent({
    changeHandler,
    streamConfigsObj
}: Props) {
    const classes = useStyles();
    const [streamConfigObj, setStreamConfigObj] = useState(streamConfigsObj);
    const [keys,setKeys] = useState(Object.keys(streamConfigObj));
    const [value,setValue] = useState(Object.values(streamConfigObj));

    const addButtonClick = ()=>{
        let data = "";
        let tempStreamConfigObj = {...streamConfigObj,
            [data]:data}
        setStreamConfigObj(tempStreamConfigObj);
        changeHandler('streamConfigs',tempStreamConfigObj)
    }

    const keyChange = (input,index, value) =>{
        input[index] = value;
        setKeys([...input]);
    }

    const valueChange = (input,index, value) =>{
        input[index] = value;
        setValue([...input]);
    }

    const updateJson = () =>{
        let configObj = {}
        keys.map((k,i)=>{
            configObj[k] = value[i]
        })
        changeHandler('streamConfigs',configObj);
    }

    const deleteClick = (val) =>{
        let tempStreamConfigObj = {...streamConfigObj};
        delete tempStreamConfigObj[val];
        setStreamConfigObj(tempStreamConfigObj);
        setKeys([]);
        setValue([]);
        changeHandler('streamConfigs',tempStreamConfigObj)
    }

    useEffect(() => {
        setStreamConfigObj(streamConfigsObj);
        setKeys(Object.keys(streamConfigObj));
        setValue(Object.values(streamConfigObj));
    }, [streamConfigsObj]);


  const requiredAstrix = <span className={classes.redColor}>*</span>;

  return (
    <Grid container>
        <Tooltip title="Configure the properties of your stream." arrow placement="top">
        <h3 className="accordion-subtitle">Stream Config</h3>
        </Tooltip>
                {
                    keys.map((o,i)=>{
                        return(
                            <Grid item xs={6}>
                                <div className="box-border">
                                <Grid container spacing={2}>
                                    <Grid item xs={6}>
                                        <FormControl className={classes.formControl}>
                                            <InputLabel htmlFor={o}>Key { compulsoryKeys.includes(o) && requiredAstrix }</InputLabel>
                                            <Input
                                                id={o}
                                                value={o}
                                                key={i+"key"}
                                                onChange={(e)=> keyChange(keys,i,e.target.value)}
                                                onBlur={updateJson}
                                            />
                                        </FormControl>
                                    </Grid>
                                    <Grid item xs={5}>
                                        <FormControl className={classes.formControl}>
                                            <InputLabel htmlFor={value[i]}>Value</InputLabel>
                                            <Input
                                                id={value[i]}
                                                value={value[i]}
                                                key={i+"value"}
                                                onChange={(e)=> valueChange(value,i,e.target.value)}
                                                onBlur={updateJson}
                                            />
                                        </FormControl>
                                    </Grid>
                                    <Grid item xs={1}>
                                        <FormControl>
                                            <IconButton aria-label="delete" key={"delete"+i} className={classes.deleteIcon} onClick={()=>{
                                                deleteClick(o)}}>
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