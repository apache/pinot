import {Grid} from "@material-ui/core";
import {RebalanceServerSection} from "../RebalanceServerSection";
import React from "react";
import './CustomCodeMirror.css';
import {RebalanceServerResponseCard} from "./RebalanceServerResponseCard";
import {RebalanceServerSectionResponse} from "./RebalanceServerSectionResponse";

export const RebalanceServerRebalanceSummaryResponse = ({ response }) => {
    const responseSectionsToShow = [
        {
            name: 'I. Server Information',
            key: 'serverInfo'
        },
        {
            name: 'II. Segment Information',
            key: 'segmentInfo'
        }
    ];

    return (
        <Grid item xs={12}>
            <RebalanceServerResponseCard>
                <RebalanceServerSection sectionTitle={"Rebalance Summary Result"} canHideSection>
                    {
                        responseSectionsToShow.map((section) => {
                            if (Object.keys(response.rebalanceSummaryResult).includes(section.key)) {
                                return <RebalanceServerSectionResponse key={section.key} sectionTitle={section.name} sectionData={response.rebalanceSummaryResult[section.key]} />
                            }
                        })
                    }
                </RebalanceServerSection>
            </RebalanceServerResponseCard>
        </Grid>
    )
}