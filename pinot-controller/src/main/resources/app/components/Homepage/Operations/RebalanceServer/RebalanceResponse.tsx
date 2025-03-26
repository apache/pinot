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