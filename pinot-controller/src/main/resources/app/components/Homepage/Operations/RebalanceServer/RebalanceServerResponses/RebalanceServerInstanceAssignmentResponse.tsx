import {Grid, Paper} from "@material-ui/core";
import {RebalanceServerSection} from "../RebalanceServerSection";
import React from "react";
import CustomCodemirror from "../../../../CustomCodemirror";
import {RebalanceServerResponseCard} from "./RebalanceServerResponseCard";

export const RebalanceServerInstanceAssignmentResponse = ({ response }) => {
    return (
        <Grid item xs={12}>
            <RebalanceServerResponseCard>
                <RebalanceServerSection sectionTitle={"Instance Assignment"} canHideSection>
                    <CustomCodemirror
                        customClass='rebalance_server_response_section'
                        data={response.instanceAssignment}
                        isEditable={false}
                    />
                </RebalanceServerSection>
            </RebalanceServerResponseCard>
        </Grid>
    );
}