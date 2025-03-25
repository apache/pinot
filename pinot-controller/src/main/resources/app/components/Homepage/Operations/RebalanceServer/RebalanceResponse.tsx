import {RebalanceServerSection} from "./RebalanceServerSection";
import React from 'react';
import {Grid, Paper} from "@material-ui/core";
import CustomCodemirror from "../../../CustomCodemirror";
import {
    RebalanceServerInstanceAssignmentResponse
} from "./RebalanceServerResponses/RebalanceServerInstanceAssignmentResponse";
import {
    RebalanceServerSegmentAssignmentResponse
} from "./RebalanceServerResponses/RebalanceServerSegmentAssignmentResponse";
import {RebalanceServerPreChecksResponse} from "./RebalanceServerResponses/RebalanceServerPreChecksResponse";
import {RebalanceServerResponseLabelValue} from "./RebalanceServerResponses/RebalanceServerResponseLabelValue";
import {
    RebalanceServerRebalanceSummaryResponse
} from "./RebalanceServerResponses/RebalanceServerRebalanceSummaryResponse";

export const RebalanceResponse = ({ response }) => {
    return (
        <Grid container spacing={2}>
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
            { response.instanceAssignment && <RebalanceServerInstanceAssignmentResponse response={response} /> }
            { response.segmentAssignment && <RebalanceServerSegmentAssignmentResponse response={response} /> }

            {/* To be kept at the last, RAW JSON Preview */}
            <Grid item xs={12}>
                <Paper variant='outlined' style={{ padding: 20 }}>
                    <RebalanceServerSection sectionTitle={"Raw JSON"} canHideSection showSectionByDefault={false}>
                        <CustomCodemirror
                            data={response}
                            isEditable={false}
                        />
                    </RebalanceServerSection>
                </Paper>
            </Grid>
        </Grid>
    );
}