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
import {RebalanceServerSection} from "./RebalanceServerSection";
import React from 'react';
import {Box, Grid, IconButton, Paper, Snackbar, Typography} from "@material-ui/core";
import CustomCodemirror from "../../../CustomCodemirror";
import {
    RebalanceServerSectionResponse
} from "./RebalanceServerResponses/RebalanceServerSectionResponse";
import {RebalanceServerPreChecksResponse} from "./RebalanceServerResponses/RebalanceServerPreChecksResponse";
import {RebalanceServerResponseLabelValue} from "./RebalanceServerResponses/RebalanceServerResponseLabelValue";
import {
    RebalanceServerRebalanceSummaryResponse
} from "./RebalanceServerResponses/RebalanceServerRebalanceSummaryResponse";
import {RebalanceServerResponseCard} from "./RebalanceServerResponses/RebalanceServerResponseCard";
import {FileCopyOutlined} from "@material-ui/icons";

const CopyJobIdToClipboardButton = ({ jobId }: { jobId: string }) => {
    const [open, setOpen] = React.useState<boolean>(false);

    const handleClick = () => {
        setOpen(true);
        navigator.clipboard.writeText(jobId);
    };

    return (
        <>
            <IconButton onClick={handleClick} color="primary">
                <FileCopyOutlined color="disabled" fontSize="small" />
            </IconButton>
            <Snackbar
                message="Copied job id to clibboard"
                anchorOrigin={{ vertical: "top", horizontal: "center" }}
                autoHideDuration={2000}
                onClose={() => setOpen(false)}
                open={open}
            />
        </>
    );
};
export const RebalanceResponse = ({ response }) => {
    const responseSectionsToShow = [
        {
            name: 'Segment Assignment',
            key: 'segmentAssignment'
        },
        {
            name: 'Instance Assignment',
            key: 'instanceAssignment'
        },
        {
            name: 'Tier Instance Assignment',
            key: 'tierInstanceAssignment'
        }
    ];

    return (
        <Grid container spacing={2} key='rebalance-response'>
            <Grid item xs={12}>
                <Paper variant='outlined' style={{ padding: 10 }}>
                    <RebalanceServerSection sectionTitle={"Job Summary"}>
                        <Grid container spacing={2}>
                            <Grid item xs={6}><RebalanceServerResponseLabelValue label='Description' value={response.description} /></Grid>
                            <Grid item xs={6}><RebalanceServerResponseLabelValue label='Status' value={response.status} /></Grid>
                            <Grid item xs={6}>
                                <Typography color='textSecondary' variant='caption'>Job Id</Typography>
                                <Box flexDirection="row" display="flex" alignItems="center" marginTop={-1.25}>
                                    <Typography style={{ fontWeight: 600 }} variant='body2'>
                                        {response.jobId}
                                    </Typography>
                                    <CopyJobIdToClipboardButton jobId={response.jobId} />
                                </Box>
                            </Grid>
                        </Grid>
                    </RebalanceServerSection>
                </Paper>
            </Grid>
            { response.preChecksResult && <RebalanceServerPreChecksResponse response={response} /> }
            { response.rebalanceSummaryResult && <RebalanceServerRebalanceSummaryResponse response={response} /> }
            {
                responseSectionsToShow.map((section) => {
                    if (Object.keys(response).includes(section.key)) {
                        return (
                            <Grid item xs={12} key={section.key}>
                                <RebalanceServerResponseCard>
                                    <RebalanceServerSectionResponse key={section.key} sectionTitle={section.name} sectionData={response[section.key]} />
                                </RebalanceServerResponseCard>
                            </Grid>
                        );
                    }
                })
            }

            {/* To be kept at the last, RAW JSON Preview */}
            <Grid item xs={12}>
                <RebalanceServerResponseCard>
                    <RebalanceServerSection sectionTitle={"Raw JSON"} canHideSection showSectionByDefault={false}>
                        <CustomCodemirror
                            data={response}
                            isEditable={false}
                        />
                    </RebalanceServerSection>
                </RebalanceServerResponseCard>
            </Grid>
        </Grid>
    );
}