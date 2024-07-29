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
import { CircularProgress, createStyles, DialogContent, DialogContentText, Link, makeStyles, Paper, Tab, Table, TableBody, TableCell, TableContainer, TableHead, TableRow, Tabs, Theme, Tooltip, withStyles} from '@material-ui/core';
import Dialog from '../../CustomDialog';
import CloseIcon from '@material-ui/icons/Close';
import CheckIcon from '@material-ui/icons/Check';
import { TableData, TableSegmentJobs } from 'Models';
import TabPanel from '../../TabPanel';
import moment from 'moment';
import CustomDialog from '../../CustomDialog';
import PinotMethodUtils from '../../../utils/PinotMethodUtils';
import CustomCodemirror from '../../CustomCodemirror';
import SearchBar from '../../SearchBar';
import RemoveCircleIcon from '@material-ui/icons/RemoveCircle';

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    root: {
      textAlign: 'center'
    },
    container: {
      height: 540,
    },
    greenColor: {
      color: theme.palette.success.main
    },
    redColor: {
      color: theme.palette.error.main
    },
    jobDetailsCodeMirror: {
      '& .CodeMirror': { maxHeight: 400, fontSize: '1rem' },
    },
    jobDetailsLoading: {
      alignSelf: "center"
    },
    yellowColor: {
      color: theme.palette.warning.main
    }
  })
);

const StyledTableCell = withStyles((theme: Theme) =>
  createStyles({
    head: {
      backgroundColor: '#ecf3fe',
      color: theme.palette.primary.main,
      fontWeight: 600
    }
  }),
)(TableCell);

type Props = {
  reloadStatusData: any,
  tableJobsData: TableSegmentJobs | null,
  hideModal: (event: React.MouseEvent<HTMLElement, MouseEvent>) => void
};

export default function ReloadStatusOp({
  reloadStatusData,
  tableJobsData,
  hideModal
}: Props) {
  const classes = useStyles();
  const reloadStatusKey = "columnToIndexesCount"
  const indexes = reloadStatusData && reloadStatusData[reloadStatusKey];
  const indexesKeys = indexes && Object.keys(indexes);
  const indexObjKeys = indexes && indexes[indexesKeys[0]] && Object.keys(indexes[indexesKeys[0]]) || [];
  const [activeTab, setActiveTab] = useState(0);
  const [segmentJobsTableData, setSegmentJobsTableData] = useState<TableData>({ records: [], columns: [] });
  const [filteredSegmentJobsTableData, setFilteredSegmentJobsTableData] = useState<TableData>({ records: [], columns: [] });
  const [jobDetailsDialogOpen, setJobDetailsDialogOpen] = useState<boolean>(false);
  const [selectedJobId, setSelectedSegmentJobId] = useState<string | null>(null);
  const [segmentJobDetails, setSegmentJobDetails] = useState(null);

  useEffect(() => {
    if(!tableJobsData) {
      return;
    }

    const segmentJobsTableData: TableData = {
      columns: ["Job Id", "Message Count", "Job Type", "Submitted On"],
      records: Object.values(tableJobsData)
        .sort((a, b) => b.submissionTimeMs - a.submissionTimeMs)
        .map((job) => [
          job.jobId,
          job.messageCount,
          job.jobType,
          moment(+job.submissionTimeMs).format("MMMM Do YYYY, HH:mm:ss"),
        ]),
    };

    setSegmentJobsTableData(segmentJobsTableData);
    setFilteredSegmentJobsTableData(segmentJobsTableData);
  }, [tableJobsData]);

  useEffect(() => {
    if(!selectedJobId) {
      return;
    }
    fetchSegmentReloadStatusData(selectedJobId);
  }, [selectedJobId]);

  const fetchSegmentReloadStatusData = async (jobId: string) => {
    const segmentJobDetails = await PinotMethodUtils.fetchSegmentReloadStatus(jobId);
    setSegmentJobDetails(segmentJobDetails)
  }

  const handleSegmentJobIdClick = (id: string) => {
    if(!id) {
      return;
    }
    setJobDetailsDialogOpen(true);
    setSegmentJobDetails(null);
    setSelectedSegmentJobId(id);
  }

  const handleJobDetailsDialogClose = () => {
    setJobDetailsDialogOpen(false);
    setSelectedSegmentJobId(null);
  }

  const handleActiveTabChange = (_: React.ChangeEvent<{}>, newIndex: number) => {
    setActiveTab(newIndex);
  }

  const handleJobIdSearchChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const searchText = e.target.value;
    const filteredRecords = segmentJobsTableData.records.filter((record) => {
      return (record[0] as string).toLowerCase().includes(searchText.toLowerCase());
    });

    setFilteredSegmentJobsTableData((data) => ({...data, records: filteredRecords}));
  }

  return (
    <Dialog
      open={true}
      handleClose={hideModal}
      title="Reload Status"
      showOkBtn={false}
      size='lg'
    >
      {!(reloadStatusData && tableJobsData) ?
        <div className={classes.root}><CircularProgress/></div>
      :
        <DialogContent>
          <Paper variant='outlined' >
          <Tabs
            value={activeTab}
            onChange={handleActiveTabChange}
            indicatorColor="primary"
            textColor="primary"
            centered
          >
            <Tab label="Jobs" />
            <Tab label="Status" />
          </Tabs>
          </Paper>
          <TabPanel value={activeTab} index={0}>
            <TableContainer component={Paper} className={classes.container}>
              <SearchBar placeholder="Search by job id" onChange={handleJobIdSearchChange} />
              <Table stickyHeader aria-label="sticky table" size="small">
                <TableHead>
                  <TableRow>
                    {filteredSegmentJobsTableData.columns.map((o, i)=>{
                      return (
                        <StyledTableCell key={i} align="left">{o}</StyledTableCell>
                      );
                    })}
                  </TableRow>
                </TableHead>
                <TableBody>
                  {filteredSegmentJobsTableData.records.map((segmentJob, index) => (
                    <TableRow key={index}>
                      {segmentJob.map((data, idx) => {
                        if(idx === 0) {
                          return (
                            <StyledTableCell align="left" key={idx}>
                              <Link underline='always' component="button" variant="body2" onClick={() => handleSegmentJobIdClick(data as string)}>{data}</Link>
                            </StyledTableCell>
                          )
                        }
                        return (
                          <StyledTableCell align="left" key={idx}>
                            {data}
                          </StyledTableCell>
                        )
                      })}
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </TabPanel>
          <TabPanel value={activeTab} index={1}>
            {indexes && indexesKeys && indexObjKeys ?
              <TableContainer component={Paper} className={classes.container}>
                <Table stickyHeader aria-label="sticky table" size="small">
                  <TableHead>
                    <TableRow>
                      <StyledTableCell></StyledTableCell>
                      {indexObjKeys.map((o, i)=>{
                        return (
                          <StyledTableCell key={i} align="right">{o}</StyledTableCell>
                        );
                      })}
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {indexesKeys.map((indexName, i) => {
                      const indexObj = indexes[indexName];
                      return (
                        <TableRow key={i}>
                          <StyledTableCell component="th" scope="row">
                            {indexName}
                          </StyledTableCell>
                          {indexObjKeys.map((o, i)=>{
                            let iconElement = null;
                            if(indexObj[o] === 0){
                              iconElement = <CloseIcon className={classes.redColor}/>;
                            } else if(indexObj[o] === reloadStatusData?.["totalOnlineSegments"]) {
                              iconElement = <CheckIcon className={classes.greenColor}/>;
                            } else {
                              const tooltipText = `Index present in ${indexObj[o]} / ${reloadStatusData?.["totalOnlineSegments"]} segments`;
                              iconElement = <Tooltip placement='top' title={tooltipText}><RemoveCircleIcon className={classes.yellowColor} /></Tooltip>
                            }
                            return (
                              <StyledTableCell align="center" key={i}>
                                {iconElement}
                              </StyledTableCell>
                            )
                          })}
                        </TableRow>
                      )
                    })}
                  </TableBody>
                </Table>
              </TableContainer>
            :
              <DialogContentText>No reload status found in table.</DialogContentText>
            }
          </TabPanel>
          <CustomDialog 
            showOkBtn={false} 
            title="Segment Job Details" 
            open={jobDetailsDialogOpen} 
            handleClose={handleJobDetailsDialogClose}
          >
            {segmentJobDetails ? (
                <CustomCodemirror
                  customClass={classes.jobDetailsCodeMirror}
                  data={segmentJobDetails}
                  isEditable={false}
                />
              ) : (
                <CircularProgress className={classes.jobDetailsLoading} />
              )
            }
          </CustomDialog>
        </DialogContent>
      }
    </Dialog>
  );
}