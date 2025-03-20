import {RebalanceServerSection} from "./RebalanceServerSection";
import React, {ReactNode} from 'react';
import {Box, Grid, Paper, Typography} from "@material-ui/core";
import InfoOutlinedIcon from "@material-ui/icons/InfoOutlined";
import Alert from "@material-ui/lab/Alert";
import {Cancel, CheckCircle} from "@material-ui/icons";
import CustomCodemirror from "../../../CustomCodemirror";

const LabelTextValueContainer = (
    { label, text }: { label: string; text?: string; }
) => {
    return (
        <Box>
            <Typography color='textSecondary' variant='caption'>{label}</Typography>
            <Typography style={{ fontWeight: 600 }} variant='body2'>{text}</Typography>
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
                            <Grid item xs={6}><LabelTextValueContainer label='Description' text={response.description} /></Grid>
                            <Grid item xs={6}><LabelTextValueContainer label='Status' text={response.status} /></Grid>
                            <Grid item xs={6}><LabelTextValueContainer label='Job Id' text={response.jobId} /></Grid>
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
                                <Alert style={{ marginBottom: 20 }} color='info' icon={<InfoOutlinedIcon fontSize='small' />}>
                                    <Typography variant='caption'>
                                        These are non-blocking checks.
                                        Rebalance can be run even if these fail. Please be sure to fix the issues before proceeding with actual rebalance!
                                    </Typography>
                                </Alert>
                                <Grid container spacing={2}>
                                    { Object.keys(response.preChecksResult).map((preCheckResult, index) => (
                                        <Grid item xs={12} style={{ display: 'flex', alignItems: 'flex-start' }} spacing={2}>
                                            {response.preChecksResult[preCheckResult].preCheckStatus === "PASS" ? <CheckCircle style={{ marginRight: 10, marginTop: 5 }} fontSize='small' htmlColor='green' /> : <Cancel style={{ marginRight: 10, marginTop: 5 }} fontSize='small' color='error' />}
                                            <LabelTextValueContainer label={preCheckResult} text={response.preChecksResult[preCheckResult].message} />
                                        </Grid>)
                                    )}
                                </Grid>
                            </RebalanceServerSection>
                        </Paper>
                    </Grid>
                )
            }
            { response.instanceAssignment &&
                <Grid item xs={12}>
                    <Paper variant='outlined' style={{ padding: 20 }}>
                        <RebalanceServerSection sectionTitle={"Instance Assignment"}>
                            { Object.keys(response.instanceAssignment).map(partitionType => (
                                <Grid container spacing={2}>
                                    <Grid item xs={12}><LabelTextValueContainer label='Instance Partitions' text={response.instanceAssignment[partitionType].instancePartitionsName} /></Grid>
                                    {
                                        Object.keys(response.instanceAssignment[partitionType].partitionToInstancesMap).map(partition => (
                                            <Grid item xs={12}>
                                                <LabelTextValueContainer
                                                    label='Partition Map'
                                                    text={[partition, response.instanceAssignment[partitionType].partitionToInstancesMap[partition].join(",")].join(" : ")} />
                                            </Grid>
                                        ))
                                    }

                                </Grid>
                            )
                        )}
                        </RebalanceServerSection>
                    </Paper>
                </Grid>
            }
            { response.segmentAssignment &&
                <Grid item xs={12}>
                  <Paper variant='outlined' style={{ padding: 20 }}>
                    <RebalanceServerSection sectionTitle={"Segment Assignment"}>
                        { Object.keys(response.segmentAssignment).map(
                            segmentId => (
                                <Grid container spacing={2}>
                                    <Grid item xs={6}><LabelTextValueContainer label='Segment Id' text={segmentId} /></Grid>
                                    <Grid item xs={6}><LabelTextValueContainer label='Assigned To' text={Object.keys(response.segmentAssignment[segmentId]).join(", ")} /></Grid>
                                </Grid>
                            )
                        )}
                    </RebalanceServerSection>
                  </Paper>
                </Grid>
            }
            {/* To be kept at the last */}
            <Grid item xs={12}>
                <Paper variant='outlined' style={{ padding: 20 }}>
                    <RebalanceServerSection sectionTitle={"Raw JSON"} canHideSection showSectionByDefault>
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