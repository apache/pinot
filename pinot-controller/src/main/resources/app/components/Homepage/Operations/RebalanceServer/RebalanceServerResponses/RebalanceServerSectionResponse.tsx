import {Grid} from "@material-ui/core";
import {RebalanceServerSection} from "../RebalanceServerSection";
import React from "react";
import CustomCodemirror from "../../../../CustomCodemirror";

export const RebalanceServerSectionResponse = ({ sectionData, sectionTitle }) => {
    return (
        <Grid item xs={12}>
            <RebalanceServerSection sectionTitle={sectionTitle} canHideSection>
                <CustomCodemirror
                    customClass='rebalance_server_response_section'
                    data={sectionData}
                    isEditable={false}
                />
            </RebalanceServerSection>
        </Grid>
    );
}