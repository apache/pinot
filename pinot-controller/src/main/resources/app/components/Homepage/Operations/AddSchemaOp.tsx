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
import { createStyles, DialogContent, Grid, makeStyles, Theme} from '@material-ui/core';
import Dialog from '../../CustomDialog';
import SimpleAccordion from '../../SimpleAccordion';
import SchemaComponent from './SchemaComponent';
import CustomCodemirror from '../../CustomCodemirror';
import PinotMethodUtils from '../../../utils/PinotMethodUtils';
import { NotificationContext } from '../../Notification/NotificationContext';
import _ from 'lodash';
import SchemaNameComponent from './SchemaNameComponent';
import CustomizedTables from '../../Table';

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

type Props = {
  hideModal: (event: React.MouseEvent<HTMLElement, MouseEvent>) => void,
  fetchData: Function
};

export default function AddSchemaOp({
  hideModal,
  fetchData
}: Props) {
  const classes = useStyles();
  const [schemaObj, setSchemaObj] = useState({schemaName:'', dateTimeFieldSpecs: []});
  const {dispatch} = React.useContext(NotificationContext);
  let isError = false

    const returnValue = (data,key) =>{
        Object.keys(data).map(async (o)=>{
          if(!_.isEmpty(data[o]) && typeof data[o] === "object"){
            await returnValue(data[o],key);
          }
          else if(!_.isEmpty(data[o]) && _.isArray(data[o])){
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

      const isObjEmpty = () =>{
        const types = ["dimensionFieldSpecs","metricFieldSpecs","dateTimeFieldSpecs"];
        let notEmpty = true;
        types.map((t)=>{
          if(schemaObj[t].length)
          {
            notEmpty = false
          }
        })
        return notEmpty;
      }

  const validateSchema = async () => {
    const validSchema = await PinotMethodUtils.validateSchemaAction(schemaObj);
    if(validSchema.error || typeof validSchema === 'string'){
      dispatch({
        type: 'error',
        message: validSchema.error || validSchema,
        show: true
      });
      return false;
    }
    return true;
  };

  const handleSave = async () => {
    const fields = [{key:"schemaName",label:"schema Name"},{key:"name",label:"Column Name"},{key:"dataType",label:"Data Type"}];
    await checkFields(schemaObj,fields);
    if(isError){
      isError = false;
      return false;
    }
    if(!isObjEmpty()){
    if(await validateSchema()){
      const schemaCreationResp = await PinotMethodUtils.saveSchemaAction(schemaObj);
      dispatch({
        type: (schemaCreationResp.error || typeof schemaCreationResp === 'string') ? 'error' : 'success',
        message: schemaCreationResp.error || schemaCreationResp.status || schemaCreationResp,
        show: true
      });
      if(!schemaCreationResp.error && typeof schemaCreationResp !== 'string'){
        fetchData();
        hideModal(null);
      }
    }
    }else{
        dispatch({
          type: 'error',
          message: "Please Enter atleast one Type",
          show: true
        });
      }
  };

  return (
    <Dialog
      open={true}
      handleClose={hideModal}
      handleSave={handleSave}
      title="Add Schema"
      size="xl"
      disableBackdropClick={true}
      disableEscapeKeyDown={true}
    >
      <DialogContent>
        <Grid container spacing={2}>
          <Grid item xs={12}>
            <SimpleAccordion
              headerTitle="Add Schema"
              showSearchBox={false}
            >
              <SchemaComponent
                schemaObj={schemaObj}
                schemaName={schemaObj.schemaName}
                setSchemaObj={(o)=>{setSchemaObj(o);}}
              />
            </SimpleAccordion>
          </Grid>
          <Grid item xs={6}>
            <div className={classes.sqlDiv}>
              <SimpleAccordion
                headerTitle="Schema Config (Read Only)"
                showSearchBox={false}
              >
                <CustomCodemirror
                  customClass={classes.queryOutput}
                  data={schemaObj}
                  isEditable={false}
                  returnCodemirrorValue={(newValue)=>{
                    try{
                      const jsonObj = JSON.parse(newValue);
                      if(jsonObj){
                        jsonObj.segmentsConfig.replicasPerPartition = jsonObj.segmentsConfig.replication;
                        setSchemaObj(jsonObj);
                      }
                    }catch(e){}
                  }}
                />
              </SimpleAccordion>
            </div>
          </Grid>
        </Grid>
      </DialogContent>
    </Dialog>
  );
}