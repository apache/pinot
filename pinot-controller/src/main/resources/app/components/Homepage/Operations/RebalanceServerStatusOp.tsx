import {
    Button, DialogContent, Grid
} from "@material-ui/core";
import Dialog from "../../CustomDialog";
import React, {useState} from "react";
import moment from "moment/moment";
import {RebalanceServerSection} from "./RebalanceServer/RebalanceServerSection";
import CustomCodemirror from "../../CustomCodemirror";
import './RebalanceServer/RebalanceServerResponses/CustomCodeMirror.css';
import {RebalanceServerResponseCard} from "./RebalanceServer/RebalanceServerResponses/RebalanceServerResponseCard";
import CustomizedTables from "../../Table";
import {TableData} from "Models";

export type RebalanceTableSegmentJobs = {
    [key: string]: {
        jobId: string,
        messageCount: number,
        submissionTimeMs: number,
        jobType: string,
        tableName: string,
        REBALANCE_PROGRESS_STATS: string,
        REBALANCE_CONTEXT: string;
    }
}

type RebalanceServerStatusOpProps = {
    rebalanceServerStatus: RebalanceTableSegmentJobs;
    hideModal: () => void;
};

export const RebalanceServerStatusOp = (
    { rebalanceServerStatus, hideModal } : RebalanceServerStatusOpProps
) => {
    const [jobSelected, setJobSelected] = useState<string | null>(null);

    const BackAction = () => {
        return <Button style={{ textTransform: 'none' }} variant='outlined' color='primary' onClick={() => setJobSelected(null)}>Back</Button>
    }
    return (
        <Dialog
            open={true}
            handleClose={hideModal}
            title="Rebalance Table Status"
            showOkBtn={false}
            size='lg'
            moreActions={jobSelected ? <BackAction /> : null}
        >
            <DialogContent>
                {
                    !jobSelected ?
                        <CustomizedTables
                            title='Job Status'
                            isCellClickable
                            makeOnlyFirstCellClickable
                            cellClickCallback={(cell: string) => {
                                setJobSelected(cell);
                            }}
                            data={{
                                records: Object.keys(rebalanceServerStatus).map(jobId => {
                                    const progressStats = JSON.parse(rebalanceServerStatus[jobId].REBALANCE_PROGRESS_STATS);
                                    return [
                                        rebalanceServerStatus[jobId].jobId,
                                        rebalanceServerStatus[jobId].tableName,
                                        progressStats.status,
                                        moment(+rebalanceServerStatus[jobId].submissionTimeMs).format("MMMM Do YYYY, HH:mm:ss")
                                    ];
                                }),
                                columns: ['Job id', 'Table name', 'Status', 'Started at']
                            }}
                            showSearchBox
                        /> :
                        <Grid container spacing={2}>
                            <Grid item xs={12}>
                                <RebalanceServerResponseCard>
                                    <RebalanceServerSection sectionTitle={"Progress Stats"} canHideSection showSectionByDefault={true}>
                                        <CustomCodemirror
                                            customClass='rebalance_server_response_section'
                                            data={JSON.parse(rebalanceServerStatus[jobSelected].REBALANCE_PROGRESS_STATS)}
                                            isEditable={false}
                                        />
                                    </RebalanceServerSection>
                                </RebalanceServerResponseCard>
                            </Grid>
                            <Grid item xs={12}>
                                <RebalanceServerResponseCard>
                                    <RebalanceServerSection sectionTitle={"Context"} canHideSection showSectionByDefault={true}>
                                        <CustomCodemirror
                                            customClass='rebalance_server_response_section'
                                            data={JSON.parse(rebalanceServerStatus[jobSelected].REBALANCE_CONTEXT)}
                                            isEditable={false}
                                        />
                                    </RebalanceServerSection>
                                </RebalanceServerResponseCard>
                            </Grid>
                        </Grid>
                }
            </DialogContent>
        </Dialog>
    );
}