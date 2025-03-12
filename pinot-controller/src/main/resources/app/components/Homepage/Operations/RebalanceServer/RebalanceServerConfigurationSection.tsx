import {Box, Typography} from "@material-ui/core";
import React, {ReactNode} from "react";

type RebalanceServerConfigurationSectionProps = {
    sectionTitle: string;
    children: ReactNode;
}
export const RebalanceServerConfigurationSection = ({sectionTitle, children}: RebalanceServerConfigurationSectionProps) => {
    return (
        <Box display='flex' flexDirection='column' marginBottom={2}>
            <Typography variant='body1' style={{fontWeight: 'bold', marginBottom: 10}}>
                {sectionTitle}
            </Typography>
            {children}
        </Box>
    );
}