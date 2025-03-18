import {Box, IconButton, Tooltip, Typography} from "@material-ui/core";
import {ReportProblemOutlined} from "@material-ui/icons";
import React from "react";
import {RebalanceServerOption} from "../../RebalanceServerOptions";
import InfoOutlinedIcon from "@material-ui/icons/InfoOutlined";

type RebalanceServerConfigurationOptionLabelProps = {
    option: RebalanceServerOption;
}
export const RebalanceServerConfigurationOptionLabel = ({option}: RebalanceServerConfigurationOptionLabelProps) => (
    <Box display='flex' flexDirection='row' alignItems='center'>
        <Typography variant='body2' style={{marginRight: 10, fontWeight: "600"}}>
            {option.label}
        </Typography>
        {option.markWithWarningIcon && (
            <Tooltip title={option.toolTip} arrow placement="right">
                <ReportProblemOutlined color='error' fontSize='small' />
            </Tooltip>
        )}
    </Box>
);