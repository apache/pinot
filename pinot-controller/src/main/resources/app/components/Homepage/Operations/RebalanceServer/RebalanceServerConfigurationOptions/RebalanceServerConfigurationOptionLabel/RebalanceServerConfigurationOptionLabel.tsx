import {Box, Typography} from "@material-ui/core";
import {ReportProblemOutlined} from "@material-ui/icons";
import React from "react";
import {RebalanceServerOption} from "../../RebalanceServerOptions";

type RebalanceServerConfigurationOptionLabelProps = {
    option: RebalanceServerOption;
}
export const RebalanceServerConfigurationOptionLabel = ({option}: RebalanceServerConfigurationOptionLabelProps) => (
    <Box display='flex' flexDirection='row' alignItems='center'>
        <Typography variant='body2' style={{marginRight: 10, fontWeight: "bold"}}>
            {option.label}
        </Typography>
        {option.hasBreakingChange && <ReportProblemOutlined color='error' fontSize='small' />}
    </Box>
);