import {Grid} from "@material-ui/core";
import {RebalanceServerSection} from "../RebalanceServerSection";
import React from "react";
import CustomCodemirror from "../../../../CustomCodemirror";
import './CustomCodeMirror.css';
import {RebalanceServerResponseCard} from "./RebalanceServerResponseCard";

export const RebalanceServerRebalanceSummaryResponse = ({ response }) => {
    return (
        <Grid item xs={12}>
            <RebalanceServerResponseCard>
                <RebalanceServerSection sectionTitle={"Rebalance Summary Result"} canHideSection>
                    <RebalanceServerSection sectionTitle={"I. Server Information"} canHideSection>
                        <CustomCodemirror
                            customClass='rebalance_server_response_section'
                            data={response.rebalanceSummaryResult.serverInfo}
                            isEditable={false}
                        />
                    </RebalanceServerSection>
                    <RebalanceServerSection sectionTitle={"II. Segment Information"} canHideSection>
                        <CustomCodemirror
                            customClass='rebalance_server_response_section'
                            data={response.rebalanceSummaryResult.segmentInfo}
                            isEditable={false}
                        />
                    </RebalanceServerSection>
                </RebalanceServerSection>
            </RebalanceServerResponseCard>
        </Grid>
    )
}