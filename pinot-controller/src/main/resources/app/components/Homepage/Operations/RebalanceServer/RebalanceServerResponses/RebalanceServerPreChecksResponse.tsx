import {Grid, Typography} from "@material-ui/core";
import {RebalanceServerSection} from "../RebalanceServerSection";
import Alert from "@material-ui/lab/Alert";
import InfoOutlinedIcon from "@material-ui/icons/InfoOutlined";
import {Cancel, CheckCircle, Warning} from "@material-ui/icons";
import React from "react";
import {RebalanceServerResponseLabelValue} from "./RebalanceServerResponseLabelValue";
import {RebalanceServerResponseCard} from "./RebalanceServerResponseCard";

const PreCheckStatusIcon = ({ preCheckStatus } : { preCheckStatus: "PASS" | "WARN" | "ERROR" }) => {
    switch (preCheckStatus) {
        case "PASS":
            return <CheckCircle style={{ marginRight: 10, marginTop: 5 }} fontSize='small' htmlColor='green' />;
        case "ERROR":
            return <Cancel style={{ marginRight: 10, marginTop: 5 }} fontSize='small' color='error' />;
        case "WARN":
            return <Warning style={{ marginRight: 10, marginTop: 5 }} fontSize='small' htmlColor='orange' />;
    }
}

export const RebalanceServerPreChecksResponse = ({ response }) => {
    const numberOfPreChecksPassing = Object.keys(response.preChecksResult ?? {})
        .filter(result => response.preChecksResult[result].preCheckStatus === 'PASS').length;
    const totalNumberOfPreChecks = Object.keys(response.preChecksResult ?? {}).length;
    return (
        <Grid item xs={12}>
            <RebalanceServerResponseCard>
                <RebalanceServerSection
                    canHideSection
                    sectionTitle={"Pre Checks Result"}
                    additionalSectionTitle={
                        <Typography variant='body2' style={{ color: numberOfPreChecksPassing === totalNumberOfPreChecks ? 'green' : 'red' }}>
                            {numberOfPreChecksPassing} / {totalNumberOfPreChecks}
                        </Typography>
                    }>
                    <Alert style={{ marginBottom: 20 }} color='info' icon={<InfoOutlinedIcon fontSize='small' />}>
                        <Typography variant='caption'>
                            These are non-blocking checks.
                            Rebalance can be run even if these fail. Please be sure to fix the issues before proceeding with actual rebalance!
                        </Typography>
                    </Alert>
                    <Grid container spacing={2}>
                        { Object.keys(response.preChecksResult).map((preCheckResult, index) => (
                            <Grid item xs={12} key={preCheckResult} style={{ display: 'flex', alignItems: 'flex-start' }}>
                                <PreCheckStatusIcon preCheckStatus={response.preChecksResult[preCheckResult].preCheckStatus} />
                                <RebalanceServerResponseLabelValue label={preCheckResult} value={response.preChecksResult[preCheckResult].message} />
                            </Grid>)
                        )}
                    </Grid>
                </RebalanceServerSection>
            </RebalanceServerResponseCard>
        </Grid>
    )
}