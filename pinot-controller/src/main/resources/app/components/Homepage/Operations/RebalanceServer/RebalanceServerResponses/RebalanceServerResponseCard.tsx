import {Paper} from "@material-ui/core";
import React, {ReactNode} from "react";

export const RebalanceServerResponseCard = (
    { children }: { children: ReactNode }
) => {
    return (
        <Paper variant='outlined' style={{ padding: 10 }}>
            { children }
        </Paper>
    );
}