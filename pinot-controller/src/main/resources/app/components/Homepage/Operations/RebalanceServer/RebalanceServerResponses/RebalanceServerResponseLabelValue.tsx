import {Box, Typography} from "@material-ui/core";
import React from "react";

export const RebalanceServerResponseLabelValue = (
    { label, value }: { label: string; value: string; }
) => {
    return (
        <Box>
            <Typography color='textSecondary' variant='caption'>{label}</Typography>
            <Typography style={{ fontWeight: 600 }} variant='body2'>{value}</Typography>
        </Box>
    );
}
