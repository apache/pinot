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
import { Button, ButtonGroup, createStyles, DialogContent, Grid, makeStyles, Theme, Tooltip} from '@material-ui/core';
import Dialog from '../../CustomDialog';
import SimpleAccordion from '../../SimpleAccordion';
import SchemaComponent from './SchemaComponent';
import CustomCodemirror from '../../CustomCodemirror';
import PinotMethodUtils from '../../../utils/PinotMethodUtils';
import { NotificationContext } from '../../Notification/NotificationContext';
import { isEmpty, isArray } from 'lodash';
import InfoOutlinedIcon from '@material-ui/icons/InfoOutlined';

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

const defaultSchemaConfig = {
  schemaName:'', 
  dimensionFieldSpecs: [], 
  metricFieldSpecs: [], 
  dateTimeFieldSpecs: []
};

enum EditView {
  SIMPLE = "SIMPLE",
  JSON = "JSON"
}

const DialogTitle = ({ editView, setEditView }) => (
  <div style={{ display: "flex", alignItems: "center", gap: "5px" }}>
      Add Schema{" "}
      <Tooltip
          arrow
          interactive
          placement="top"
          title={
              <a
                  className="tooltip-link"
                  href="https://docs.pinot.apache.org/configuration-reference/schema"
                  rel="noreferrer"
                  target="_blank"
              >
                  Click here for more details
              </a>
          }
      >
          <InfoOutlinedIcon />
      </Tooltip>
      <ButtonGroup style={{marginLeft: "16px"}} size="small" color="primary">
        <Button
            variant={editView === EditView.SIMPLE ? "contained" : "outlined"}
            onClick={() => setEditView(EditView.SIMPLE)}
        >
            Simple
        </Button>
        <Button
            variant={editView === EditView.JSON ? "contained" : "outlined"}
            onClick={() => setEditView(EditView.JSON)}
        >
            Json
        </Button>
    </ButtonGroup>
  </div>
);

export default function AddSchemaOp({
  hideModal,
  fetchData
}: Props) {
  const [editView, setEditView] = useState<EditView>(EditView.SIMPLE);
  const classes = useStyles();
  const [schemaObj, setSchemaObj] = useState({...defaultSchemaConfig});
  const [jsonSchema, setJsonSchema] = useState({...defaultSchemaConfig});
  const {dispatch} = React.useContext(NotificationContext);
  const schemaConfig = editView === EditView.SIMPLE ? schemaObj : jsonSchema;
  let isError = false;

  useEffect(() => {
    // reset schema config when view changes
    setSchemaObj({...defaultSchemaConfig});
    setJsonSchema({...defaultSchemaConfig});
  }, [editView]);

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

      const isObjEmpty = () =>{
        const types = ["dimensionFieldSpecs","metricFieldSpecs","dateTimeFieldSpecs"];
        let notEmpty = true;
        types.map((t)=>{
          if(schemaConfig[t].length)
          {
            notEmpty = false
          }
        })
        return notEmpty;
      }

  const validateSchema = async () => {
    const validSchema = await PinotMethodUtils.validateSchemaAction(schemaConfig);
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
    await checkFields(schemaConfig,fields);
    if(isError){
      isError = false;
      return false;
    }
    if(!isObjEmpty()){
    if(await validateSchema()){
      const schemaCreationResp = await PinotMethodUtils.saveSchemaAction(schemaConfig);
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
      title={<DialogTitle editView={editView} setEditView={setEditView} />}
      size="xl"
      disableBackdropClick={true}
      disableEscapeKeyDown={true}
    >
      <DialogContent>
        {editView === EditView.SIMPLE && (
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
        )}

        {editView === EditView.JSON && (
          <CustomCodemirror 
            data={jsonSchema} 
            isEditable={true} 
            returnCodemirrorValue={(newValue)=>{
              try{
                const jsonSchema = JSON.parse(newValue);
                if(jsonSchema){
                  setJsonSchema(jsonSchema);
                }
              }catch(e){}
            }} 
          />
        )}
      </DialogContent>
    </Dialog>
  );
}