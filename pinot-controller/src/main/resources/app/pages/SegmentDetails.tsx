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

import React, { useState, useEffect } from 'react';
import { makeStyles } from '@material-ui/core/styles';
import { Grid } from '@material-ui/core';
import { RouteComponentProps, useHistory, useLocation } from 'react-router-dom';
import { UnControlled as CodeMirror } from 'react-codemirror2';
import AppLoader from '../components/AppLoader';
import TableToolbar from '../components/TableToolbar';
import 'codemirror/lib/codemirror.css';
import 'codemirror/theme/material.css';
import 'codemirror/mode/javascript/javascript';
import 'codemirror/mode/sql/sql';
import SimpleAccordion from '../components/SimpleAccordion';
import CustomizedTables from '../components/Table';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import CustomButton from '../components/CustomButton';
import Confirm from '../components/Confirm';
import { NotificationContext } from '../components/Notification/NotificationContext';
import Utils from '../utils/Utils';

const useStyles = makeStyles((theme) => ({
  root: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: '20px',
  },
  highlightBackground: {
    border: '1px #4285f4 solid',
    backgroundColor: 'rgba(66, 133, 244, 0.05)',
    borderRadius: 4,
    marginBottom: '20px',
  },
  body: {
    borderTop: '1px solid #BDCCD9',
    fontSize: '16px',
    lineHeight: '3rem',
    paddingLeft: '15px',
  },
  queryOutput: {
    border: '1px solid #BDCCD9',
    '& .CodeMirror': { height: 532 },
  },
  sqlDiv: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: '20px',
  },
  operationDiv: {
    border: '1px #BDCCD9 solid',
    borderRadius: 4,
    marginBottom: 20
  }
}));

const jsonoptions = {
  lineNumbers: true,
  mode: 'application/json',
  styleActiveLine: true,
  gutters: ['CodeMirror-lint-markers'],
  theme: 'default',
  readOnly: true
};

type Props = {
  tenantName: string;
  tableName: string;
  segmentName: string;
};

type Summary = {
  segmentName: string;
  totalDocs: string | number;
  createTime: unknown;
};

const SegmentDetails = ({ match }: RouteComponentProps<Props>) => {
  const classes = useStyles();
  const history = useHistory();
  const location = useLocation();
  const { tableName, segmentName: encodedSegmentName} = match.params;
  const segmentName = Utils.encodeString(encodedSegmentName);

  const [fetching, setFetching] = useState(true);
  const [confirmDialog, setConfirmDialog] = React.useState(false);
  const [dialogDetails, setDialogDetails] = React.useState(null);
  const {dispatch} = React.useContext(NotificationContext);

  const [segmentSummary, setSegmentSummary] = useState<Summary>({
    segmentName,
    totalDocs: '',
    createTime: '',
  });

  const [replica, setReplica] = useState({
    columns: [],
    records: []
  });

  const [indexes, setIndexes] = useState({
      columns: [],
      records: []
  });
  const [value, setValue] = useState('');
  const fetchData = async () => {
    const result = await PinotMethodUtils.getSegmentDetails(tableName, segmentName);
    setSegmentSummary(result.summary);
    setIndexes(result.indexes);
    setReplica(result.replicaSet);
    setValue(JSON.stringify(result.JSON, null, 2));
    setFetching(false);
  };
  useEffect(() => {
    fetchData();
  }, []);

  const closeDialog = () => {
    setConfirmDialog(false);
    setDialogDetails(null);
  };

  const handleDeleteSegmentClick = () => {
    setDialogDetails({
      title: 'Delete Segment',
      content: 'Are you sure want to delete this instance? Data from this segment will be permanently deleted.',
      successCb: () => handleDeleteSegment()
    });
    setConfirmDialog(true);
  };

  const handleDeleteSegment = async () => {
    const result = await PinotMethodUtils.deleteSegmentOp(tableName, segmentName);
    if(result && result.status){
      dispatch({type: 'success', message: result.status, show: true});
      fetchData();
    } else {
      dispatch({type: 'error', message: result.error, show: true});
    }
    closeDialog();
    setTimeout(()=>{
      history.push(Utils.navigateToPreviousPage(location, false));
    }, 1000);
  };

  const handleReloadSegmentClick = () => {
    setDialogDetails({
      title: 'Reload Segment',
      content: 'Are you sure want to reload this segment?',
      successCb: () => handleReloadOp()
    });
    setConfirmDialog(true);
  };

  const handleReloadOp = async () => {
    const result = await PinotMethodUtils.reloadSegmentOp(tableName, segmentName);
    if(result.status){
      dispatch({type: 'success', message: result.status, show: true});
      fetchData();
    } else {
      dispatch({type: 'error', message: result.error, show: true});
    }
    closeDialog();
  }

  return fetching ? (
    <AppLoader />
  ) : (
    <Grid
      item
      xs
      style={{
        padding: 20,
        backgroundColor: 'white',
        maxHeight: 'calc(100vh - 70px)',
        overflowY: 'auto',
      }}
    >
      <div className={classes.operationDiv}>
        <SimpleAccordion
          headerTitle="Operations"
          showSearchBox={false}
        >
          <div>
            <CustomButton
              onClick={()=>{handleDeleteSegmentClick()}}
              tooltipTitle="Delete Segment"
              enableTooltip={true}
            >
              Delete Segment
            </CustomButton>
            <CustomButton
              onClick={()=>{handleReloadSegmentClick()}}
              tooltipTitle="Reload the segment to apply changes such as indexing, column default values, etc"
              enableTooltip={true}
            >
              Reload Segment
            </CustomButton>
          </div>
        </SimpleAccordion>
      </div>
      <div className={classes.highlightBackground}>
        <TableToolbar name="Summary" showSearchBox={false} />
        <Grid container className={classes.body}>
          <Grid item xs={6}>
            <strong>Segment Name:</strong> {unescape(segmentSummary.segmentName)}
          </Grid>
          <Grid item xs={3}>
            <strong>Total Docs:</strong> {segmentSummary.totalDocs}
          </Grid>
          <Grid item xs={3}>
            <strong>Create Time:</strong>  {segmentSummary.createTime}
          </Grid>
        </Grid>
      </div>

      <Grid container spacing={2}>
        <Grid item xs={6}>
          <CustomizedTables
            title="Replica Set"
            data={replica}
            addLinks
            baseURL="/instance/"
            showSearchBox={true}
            inAccordionFormat={true}
          />
        </Grid>
        <Grid item xs={6}>
          <div className={classes.sqlDiv}>
            <SimpleAccordion
              headerTitle="Metadata"
              showSearchBox={false}
            >
              <CodeMirror
                options={jsonoptions}
                value={value}
                className={classes.queryOutput}
                autoCursor={false}
              />
            </SimpleAccordion>
          </div>
        </Grid>

      </Grid>

              <Grid item xs={12}>
                <CustomizedTables
                  title="Indexes"
                  data={indexes}
                  showSearchBox={true}
                  inAccordionFormat={true}
                />
              </Grid>

      {confirmDialog && dialogDetails && <Confirm
        openDialog={confirmDialog}
        dialogTitle={dialogDetails.title}
        dialogContent={dialogDetails.content}
        successCallback={dialogDetails.successCb}
        closeDialog={closeDialog}
        dialogYesLabel='Yes'
        dialogNoLabel='No'
      />}
    </Grid>
  );
};

export default SegmentDetails;
