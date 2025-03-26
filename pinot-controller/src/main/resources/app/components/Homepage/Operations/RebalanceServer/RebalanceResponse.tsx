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
import {Grid, Paper} from "@material-ui/core";
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
                            <Grid item xs={6}><RebalanceServerResponseLabelValue label='Job Id' value={response.jobId} /></Grid>
                        </Grid>
                    </RebalanceServerSection>
                </Paper>
            </Grid>
            { response.preChecksResult && <RebalanceServerPreChecksResponse response={response} /> }
            { response.rebalanceSummaryResult && <RebalanceServerRebalanceSummaryResponse response={response} /> }
            {
                responseSectionsToShow.map((section) => {
                    if (Object.keys(response).includes(section.key)) {
                        return <RebalanceServerSectionResponse key={section.key} sectionTitle={section.name} sectionData={response[section.key]} />
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