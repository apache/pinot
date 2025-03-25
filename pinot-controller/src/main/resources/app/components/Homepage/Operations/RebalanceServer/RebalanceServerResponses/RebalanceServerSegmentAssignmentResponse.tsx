import {Grid, Paper} from "@material-ui/core";
import {RebalanceServerSection} from "../RebalanceServerSection";
import React from "react";
import CustomCodemirror from "../../../../CustomCodemirror";
import './CustomCodeMirror.css';
import {RebalanceServerResponseCard} from "./RebalanceServerResponseCard";

export const RebalanceServerSegmentAssignmentResponse = ({ response }) => {
    return (
        <Grid item xs={12}>
            <RebalanceServerResponseCard>
                <RebalanceServerSection sectionTitle={"Segment Assignment"} canHideSection>
                    <CustomCodemirror
                        customClass='rebalance_server_response_section'
                        data={response.segmentAssignment}
                        isEditable={false}
                    />
                </RebalanceServerSection>
            </RebalanceServerResponseCard>
        </Grid>
    )
}