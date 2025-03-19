import {RebalanceServerSection} from "./RebalanceServerSection";
import React from 'react';
import {Box, Grid, Paper, Typography} from "@material-ui/core";
import InfoOutlinedIcon from "@material-ui/icons/InfoOutlined";
import Alert from "@material-ui/lab/Alert";
import {Cancel, CheckCircle} from "@material-ui/icons";

const LabelTextContainer = ({ label, text }: { label: string; text: string; }) => {
    return (
        <Box>
            <Typography color='textSecondary' variant='caption'>{label}</Typography>
            <Typography variant='body2'>{text}</Typography>
        </Box>
    );
}


export const RebalanceResponse = ({ response }) => {
    const numberOfPreChecksPassing = Object.keys(response.preChecksResult ?? {})
        .filter(result => response.preChecksResult[result].preCheckStatus === 'PASS').length;
    const totalNumberOfPreChecks = Object.keys(response.preChecksResult ?? {}).length;

    return (
        <Grid container spacing={2}>
            <Grid item xs={12}>
                <Paper variant='outlined' style={{ padding: 20 }}>
                    <RebalanceServerSection sectionTitle={"Job Summary"}>
                        <Grid container spacing={2}>
                            <Grid item xs={6}><LabelTextContainer label='Job Id' text={response.jobId} /></Grid>
                            <Grid item xs={6}><LabelTextContainer label='Status' text={response.status} /></Grid>
                            <Grid item xs={6}><LabelTextContainer label='Description' text={response.description} /></Grid>
                        </Grid>
                    </RebalanceServerSection>
                </Paper>
            </Grid>
            {
                response.preChecksResult && (
                    <Grid item xs={12}>
                        <Paper variant='outlined' style={{ padding: 20 }}>
                            <RebalanceServerSection
                                sectionTitle={"Pre Checks Result"}
                                additionalSectionTitle={
                                <Typography variant='body2' style={{ color: numberOfPreChecksPassing === totalNumberOfPreChecks ? 'green' : 'red' }}>
                                    {numberOfPreChecksPassing} / {totalNumberOfPreChecks}
                                </Typography>
                            }>
                                <Alert style={{ marginBottom: 5 }} color='info' icon={<InfoOutlinedIcon fontSize='small' />}>
                                    <Typography variant='caption'>
                                        These are non-blocking checks.
                                        Rebalance can be run even if these fail. Please be sure to fix the issues before proceeding with actual rebalance!
                                    </Typography>
                                </Alert>
                                <Grid container spacing={2}>
                                    { Object.keys(response.preChecksResult).map(preCheckResult => (
                                        <Grid item xs={12} style={{ display: 'flex', alignItems: 'center' }} spacing={2}>
                                            {response.preChecksResult[preCheckResult].preCheckStatus === "PASS" ? <CheckCircle style={{ marginRight: 10 }} fontSize='small' htmlColor='green' /> : <Cancel style={{ marginRight: 10 }} fontSize='small' color='error' />}
                                            <LabelTextContainer label={preCheckResult} text={response.preChecksResult[preCheckResult].message} />
                                        </Grid>)
                                    )}
                                </Grid>
                            </RebalanceServerSection>
                        </Paper>
                    </Grid>
                )
            }
        </Grid>
    );
}