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
import {
    Box, Button, CircularProgress, DialogContent, Grid
} from "@material-ui/core";
import Dialog from "../../CustomDialog";
import React, {useEffect, useState} from "react";
import {RebalanceServerSection} from "./RebalanceServer/RebalanceServerSection";
import CustomCodemirror from "../../CustomCodemirror";
import './RebalanceServer/RebalanceServerResponses/CustomCodeMirror.css';
import {RebalanceServerResponseCard} from "./RebalanceServer/RebalanceServerResponses/RebalanceServerResponseCard";
import CustomizedTables from "../../Table";
import Utils from "../../../utils/Utils";
import PinotMethodUtils from "../../../utils/PinotMethodUtils";

type RebalanceTableSegmentJob = {
    jobId: string;
    messageCount: number;
    submissionTimeMs: number;
    jobType: string;
    tableName: string;
    REBALANCE_PROGRESS_STATS: string;
    REBALANCE_CONTEXT: string;
}
export type RebalanceTableSegmentJobs = {
    [key: string]: RebalanceTableSegmentJob;
}

type RebalanceServerStatusOpProps = {
    tableName: string;
    hideModal: () => void;
};

export const RebalanceServerStatusOp = (
    { tableName, hideModal } : RebalanceServerStatusOpProps
) => {
    const [rebalanceServerJobs, setRebalanceServerJobs] = React.useState<RebalanceTableSegmentJob[]>([])
    const [jobSelected, setJobSelected] = useState<string | null>(null);
    const [rebalanceContext, setRebalanceContext] = useState<{}>({});
    const [rebalanceProgressStats, setRebalanceProgressStats] = useState<{}>({});
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        setLoading(true);
        PinotMethodUtils
            .fetchTableJobs(tableName, "TABLE_REBALANCE")
            .then(jobs => {
                if (jobs.error) {
                    return;
                }
                const sortedJobs: RebalanceTableSegmentJob[] = Object.keys(jobs as RebalanceTableSegmentJobs)
                    .map(jobId => jobs[jobId] as RebalanceTableSegmentJob)
                    .sort((j1, j2) => j1.submissionTimeMs < j2.submissionTimeMs ? 1 : -1);
                setRebalanceServerJobs(sortedJobs);
            })
            .finally(() => setLoading(false));
    }, []);

    const BackAction = () => {
        return (
            <Button
                variant='outlined'
                color='primary'
                onClick={() => setJobSelected(null)}
            >
                Back
            </Button>
        );
    }

    useEffect(() => {
        try {
            if (jobSelected !== null && rebalanceServerJobs.length > 0) {
                const rebalanceServerJob = rebalanceServerJobs
                    .find(job => job.jobId === jobSelected);

                if (rebalanceServerJob) {
                    setRebalanceContext(JSON.parse(rebalanceServerJob.REBALANCE_CONTEXT));
                    setRebalanceProgressStats(JSON.parse(rebalanceServerJob.REBALANCE_PROGRESS_STATS));
                } else {
                    setRebalanceContext(
                        {
                            message: 'Failed to load rebalance context'
                        });
                    setRebalanceProgressStats(
                        {
                            message: 'Failed to load rebalance progress stats'
                        });
                }

            }
        } catch (e) {
            setRebalanceContext(
                {
                    message: 'Failed to load rebalance context' + e.toString()
                });
            setRebalanceProgressStats(
                {
                    message: 'Failed to load rebalance progress stats' + e.toString()
                });
        }
    }, [jobSelected, rebalanceServerJobs]);

    if (loading) {
        return (
            <Dialog
                open={true}
                handleClose={hideModal}
                title="Rebalance Table Status"
                showOkBtn={false}
                size='lg'
                moreActions={jobSelected ? <BackAction /> : null}
            >
                <DialogContent>
                    <Box alignItems='center' display='flex' justifyContent='center'>
                        <CircularProgress />
                    </Box>
                </DialogContent>
            </Dialog>
        )
    }

    return (
        <Dialog
            open={true}
            handleClose={hideModal}
            title="Rebalance Table Status"
            showOkBtn={false}
            size='lg'
            moreActions={jobSelected ? <BackAction /> : null}
        >
            <DialogContent>
                {
                    !jobSelected ?
                        <CustomizedTables
                            title='Job Status'
                            isCellClickable
                            makeOnlyFirstCellClickable
                            cellClickCallback={(cell: string) => {
                                setJobSelected(cell);
                            }}
                            data={{
                                records: rebalanceServerJobs.map(rebalanceServerJob => {
                                    const progressStats = JSON.parse(rebalanceServerJob.REBALANCE_PROGRESS_STATS);
                                    return [
                                        rebalanceServerJob.jobId,
                                        rebalanceServerJob.tableName,
                                        progressStats.status,
                                        Utils.formatTime(+rebalanceServerJob.submissionTimeMs)
                                    ];
                                }),
                                columns: ['Job id', 'Table name', 'Status', 'Started at']
                            }}
                            showSearchBox
                        /> :
                        <Grid container spacing={2}>
                            <Grid item xs={12}>
                                <RebalanceServerResponseCard>
                                    <RebalanceServerSection sectionTitle={"Progress Stats"} canHideSection showSectionByDefault={true}>
                                        <CustomCodemirror
                                            customClass='rebalance_server_response_section'
                                            data={rebalanceProgressStats}
                                            isEditable={false}
                                        />
                                    </RebalanceServerSection>
                                </RebalanceServerResponseCard>
                            </Grid>
                            <Grid item xs={12}>
                                <RebalanceServerResponseCard>
                                    <RebalanceServerSection sectionTitle={"Context"} canHideSection showSectionByDefault={true}>
                                        <CustomCodemirror
                                            customClass='rebalance_server_response_section'
                                            data={rebalanceContext}
                                            isEditable={false}
                                        />
                                    </RebalanceServerSection>
                                </RebalanceServerResponseCard>
                            </Grid>
                        </Grid>
                }
            </DialogContent>
        </Dialog>
    );
}