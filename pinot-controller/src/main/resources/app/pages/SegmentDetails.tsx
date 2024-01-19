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
import moment from 'moment';
import { keys } from 'lodash';
import { makeStyles } from '@material-ui/core/styles';
import { Grid } from '@material-ui/core';
import { RouteComponentProps, useHistory, useLocation } from 'react-router-dom';
import { UnControlled as CodeMirror } from 'react-codemirror2';
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
import {
  getExternalView,
  getSegmentDebugInfo,
  getSegmentMetadata,
} from '../requests';
import { SegmentMetadata } from 'Models';
import Skeleton from '@material-ui/lab/Skeleton';
import NotFound from '../components/NotFound';
import AppLoader from '../components/AppLoader';

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
    marginBottom: 20,
  },
}));

const jsonoptions = {
  lineNumbers: true,
  mode: 'application/json',
  styleActiveLine: true,
  gutters: ['CodeMirror-lint-markers'],
  theme: 'default',
  readOnly: true,
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
  const { tableName, segmentName: encodedSegmentName } = match.params;
  const segmentName = Utils.encodeString(encodedSegmentName);

  const [confirmDialog, setConfirmDialog] = React.useState(false);
  const [dialogDetails, setDialogDetails] = React.useState(null);
  const { dispatch } = React.useContext(NotificationContext);

  const [initialLoad, setInitialLoad] = useState(true);
  const [segmentNotFound, setSegmentNotFound] = useState(false);

  const initialSummary = {
    segmentName,
    totalDocs: null,
    createTime: null,
  };
  const [segmentSummary, setSegmentSummary] = useState<Summary>(initialSummary);

  const replicaColumns = ['Server Name', 'Status'];
  const initialReplica = Utils.getLoadingTableData(replicaColumns);
  const [replica, setReplica] = useState(initialReplica);

  const indexColumns = [
    'Field Name',
    'Bloom Filter',
    'Dictionary',
    'Forward Index',
    'Sorted',
    'Inverted Index',
    'JSON Index',
    'Null Value Vector Reader',
    'Range Index',
  ];
  const initialIndexes = Utils.getLoadingTableData(indexColumns);
  const [indexes, setIndexes] = useState(initialIndexes);
  const initialSegmentMetadataJson = 'Loading...';
  const [segmentMetadataJson, setSegmentMetadataJson] = useState(
    initialSegmentMetadataJson
  );

  const fetchData = async () => {
    // reset all state in case the segment was reloaded or deleted.
    setInitialData();

    getSegmentMetadata(tableName, segmentName).then((result) => {
      if (result.data?.code === 404) {
        setSegmentNotFound(true);
        setInitialLoad(false);
      } else {
        setInitialLoad(false);
        setSummary(result.data);
        setSegmentMetadata(result.data);
        setSegmentIndexes(result.data);
      }
    });
    setSegmentReplicas();
  };

  const setInitialData = () => {
    setInitialLoad(true);
    setSegmentSummary(initialSummary);
    setReplica(initialReplica);
    setIndexes(initialIndexes);
    setSegmentMetadataJson(initialSegmentMetadataJson);
  };

  const setSummary = (segmentMetadata: SegmentMetadata) => {
    const segmentMetaDataJson = { ...segmentMetadata };
    setSegmentSummary({
      segmentName,
      totalDocs: segmentMetaDataJson['segment.total.docs'] || 0,
      createTime: moment(+segmentMetaDataJson['segment.creation.time']).format(
        'MMMM Do YYYY, h:mm:ss'
      ),
    });
  };

  const setSegmentMetadata = (segmentMetadata: SegmentMetadata) => {
    const segmentMetaDataJson = { ...segmentMetadata };
    delete segmentMetaDataJson.indexes;
    delete segmentMetaDataJson.columns;
    setSegmentMetadataJson(JSON.stringify(segmentMetaDataJson, null, 2));
  };

  const setSegmentIndexes = (segmentMetadata: SegmentMetadata) => {
    setIndexes({
      columns: indexColumns,
      records: Object.keys(segmentMetadata.indexes).map((fieldName) => [
        fieldName,
        segmentMetadata.indexes?.[fieldName]?.['bloom-filter'] === 'YES',
        segmentMetadata.indexes?.[fieldName]?.['dictionary'] === 'YES',
        segmentMetadata.indexes?.[fieldName]?.['forward-index'] === 'YES',
        (
          (segmentMetadata.columns || []).filter(
            (row) => row.columnName === fieldName
          )[0] || { sorted: false }
        ).sorted,
        segmentMetadata.indexes?.[fieldName]?.['inverted-index'] === 'YES',
        segmentMetadata.indexes?.[fieldName]?.['json-index'] === 'YES',
        segmentMetadata.indexes?.[fieldName]?.['null-value-vector-reader'] ===
          'YES',
        segmentMetadata.indexes?.[fieldName]?.['range-index'] === 'YES',
      ]),
    });
  };

  const setSegmentReplicas = () => {
    let [baseTableName, tableType] = Utils.splitStringByLastUnderscore(
      tableName
    );

    getExternalView(tableName).then((results) => {
      const externalView = results.data.OFFLINE || results.data.REALTIME;

      const records = keys(externalView?.[segmentName] || {}).map((prop) => {
        const status = externalView?.[segmentName]?.[prop];
        return [
          prop,
          { value: status, tooltip: `Segment is ${status.toLowerCase()}` },
        ];
      });

      setReplica({
        columns: replicaColumns,
        records: records,
      });

      getSegmentDebugInfo(baseTableName, tableType.toLowerCase()).then(
        (debugInfo) => {
          const segmentDebugInfo = debugInfo.data;

          let debugInfoObj = {};
          if (segmentDebugInfo && segmentDebugInfo[0]) {
            const debugInfosObj = segmentDebugInfo[0].segmentDebugInfos?.find(
              (o) => {
                return o.segmentName === segmentName;
              }
            );
            if (debugInfosObj) {
              const serverNames = keys(debugInfosObj?.serverState || {});
              serverNames?.map((serverName) => {
                debugInfoObj[serverName] =
                  debugInfosObj.serverState[
                    serverName
                  ]?.errorInfo?.errorMessage;
              });
            }
          }

          const records = keys(externalView?.[segmentName] || {}).map(
            (prop) => {
              const status = externalView?.[segmentName]?.[prop];
              return [
                prop,
                status === 'ERROR'
                  ? {
                      value: status,
                      tooltip: debugInfoObj?.[prop] || 'testing',
                    }
                  : {
                      value: status,
                      tooltip: `Segment is ${status.toLowerCase()}`,
                    },
              ];
            }
          );

          setReplica({
            columns: replicaColumns,
            records: records,
          });
        }
      );
    });
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
      content:
        'Are you sure want to delete this instance? Data from this segment will be permanently deleted.',
      successCb: () => handleDeleteSegment(),
    });
    setConfirmDialog(true);
  };

  const handleDeleteSegment = async () => {
    const result = await PinotMethodUtils.deleteSegmentOp(
      tableName,
      segmentName
    );
    if (result && result.status) {
      dispatch({ type: 'success', message: result.status, show: true });
      fetchData();
    } else {
      dispatch({ type: 'error', message: result.error, show: true });
    }
    closeDialog();
    setTimeout(() => {
      history.push(Utils.navigateToPreviousPage(location, false));
    }, 1000);
  };

  const handleReloadSegmentClick = () => {
    setDialogDetails({
      title: 'Reload Segment',
      content: 'Are you sure want to reload this segment?',
      successCb: () => handleReloadOp(),
    });
    setConfirmDialog(true);
  };

  const handleReloadOp = async () => {
    const result = await PinotMethodUtils.reloadSegmentOp(
      tableName,
      segmentName
    );
    if (result.status) {
      dispatch({ type: 'success', message: result.status, show: true });
      fetchData();
    } else {
      dispatch({ type: 'error', message: result.error, show: true });
    }
    closeDialog();
  };

  if (initialLoad) {
    return <AppLoader />;
  } else if (segmentNotFound) {
    return <NotFound message={`Segment ${segmentName} not found`} />;
  } else {
    return (
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
          <SimpleAccordion headerTitle="Operations" showSearchBox={false}>
            <div>
              <CustomButton
                onClick={() => {
                  handleDeleteSegmentClick();
                }}
                tooltipTitle="Delete Segment"
                enableTooltip={true}
              >
                Delete Segment
              </CustomButton>
              <CustomButton
                onClick={() => {
                  handleReloadSegmentClick();
                }}
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
              <strong>Segment Name:</strong>{' '}
              {unescape(segmentSummary.segmentName)}
            </Grid>
            <Grid item container xs={2} wrap="nowrap" spacing={1}>
              <Grid item>
                <strong>Total Docs:</strong>
              </Grid>
              <Grid item>
                {segmentSummary.totalDocs ? (
                  segmentSummary.totalDocs
                ) : (
                  <Skeleton variant="text" animation="wave" width={50} />
                )}
              </Grid>
            </Grid>
            <Grid item container xs={4} wrap="nowrap" spacing={1}>
              <Grid item>
                <strong>Create Time:</strong>
              </Grid>
              <Grid item>
                {segmentSummary.createTime ? (
                  segmentSummary.createTime
                ) : (
                  <Skeleton variant="text" animation="wave" width={200} />
                )}
              </Grid>
            </Grid>
          </Grid>
        </div>

        <Grid container spacing={2}>
          <Grid item xs={6}>
            <CustomizedTables
              title="Replica Set"
              data={replica}
              showSearchBox={true}
              inAccordionFormat={true}
              addLinks
              baseURL="/instance/"
            />
          </Grid>
          <Grid item xs={6}>
            <div className={classes.sqlDiv}>
              <SimpleAccordion headerTitle="Metadata" showSearchBox={false}>
                <CodeMirror
                  options={jsonoptions}
                  value={segmentMetadataJson}
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

        {confirmDialog && dialogDetails && (
          <Confirm
            openDialog={confirmDialog}
            dialogTitle={dialogDetails.title}
            dialogContent={dialogDetails.content}
            successCallback={dialogDetails.successCb}
            closeDialog={closeDialog}
            dialogYesLabel="Yes"
            dialogNoLabel="No"
          />
        )}
      </Grid>
    );
  }
};

export default SegmentDetails;
