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
import { formatTimeInTimezone } from '../../../utils/TimezoneUtils';
import { useTimezone } from '../../../contexts/TimezoneContext';
import {RebalanceTableSegmentJob} from "Models";

type RebalanceServerStatusOpProps = {
    tableName: string;
    hideModal: () => void;
    initialJobId?: string;
};

export const RebalanceServerStatusOp = (
    { tableName, hideModal, initialJobId } : RebalanceServerStatusOpProps
) => {
    const { currentTimezone } = useTimezone();
    const [rebalanceServerJobs, setRebalanceServerJobs] = React.useState<RebalanceTableSegmentJob[]>([])
    const [jobSelected, setJobSelected] = useState<string | null>(null);
    const [rebalanceContext, setRebalanceContext] = useState<{}>({});
    const [rebalanceProgressStats, setRebalanceProgressStats] = useState<{}>({});
    const [loading, setLoading] = useState(false);

    const fetchRebalanceJobs = () => {
        setLoading(true);
        PinotMethodUtils
            .fetchRebalanceTableJobs(tableName)
            .then(jobs => {
                setRebalanceServerJobs(jobs)
            })
            .finally(() => setLoading(false));
    };

    useEffect(() => {
        fetchRebalanceJobs();
    }, []);

    // Set initial job selection when jobs are loaded and initialJobId is provided
    useEffect(() => {
        if (initialJobId && rebalanceServerJobs.length > 0 && !jobSelected) {
            const jobExists = rebalanceServerJobs.find(job => job.jobId === initialJobId);
            if (jobExists) {
                setJobSelected(initialJobId);
            }
        }
    }, [initialJobId, rebalanceServerJobs, jobSelected]);

    const RefreshAction = () => {
        return (
            <Button
                variant='outlined'
                color='primary'
                onClick={fetchRebalanceJobs}
                disabled={loading}
            >
                Refresh
            </Button>
        );
    };

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
    };


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
                title="Rebalance Servers Status"
                showOkBtn={false}
                size='lg'
                moreActions={
                    <Box display="flex">
                        <RefreshAction />
                        {jobSelected && !initialJobId && <Box ml={1}><BackAction /></Box>}
                    </Box>
                }
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
            title="Rebalance Servers Status"
            showOkBtn={false}
            size='lg'
            moreActions={
                <Box display="flex">
                    <RefreshAction />
                    {jobSelected && !initialJobId && <Box ml={1}><BackAction /></Box>}
                </Box>
            }
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
                                        formatTimeInTimezone(+rebalanceServerJob.submissionTimeMs + (JSON.parse(rebalanceServerJob?.REBALANCE_PROGRESS_STATS || '{}').timeToFinishInSeconds * 1000))
                                    ];
                                }),
                                columns: ['Job id', 'Table name', 'Status', 'Started at', 'Finished at']
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
