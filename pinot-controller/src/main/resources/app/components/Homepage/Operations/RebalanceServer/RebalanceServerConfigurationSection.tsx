import {Box, Typography} from "@material-ui/core";
import React, {ReactNode, useEffect, useRef, useState} from "react";
import Link from "@material-ui/core/Link";

type RebalanceServerConfigurationSectionProps = {
    sectionTitle: string;
    children: ReactNode;
    showSectionByDefault?: boolean;
    canHideSection?: boolean;
}

export const RebalanceServerConfigurationSection = (
    { sectionTitle, children, showSectionByDefault = true, canHideSection = false }: RebalanceServerConfigurationSectionProps
) => {
    const [showSection, setShowSection] = useState<boolean>(showSectionByDefault);
    const showHideSectionRef = useRef(null);

    const handleScrollToSection = () => {
        if (showHideSectionRef.current) {
            showHideSectionRef.current.scrollIntoView(
                {
                    behavior: 'smooth',
                    block: 'start',
                });
        }
    };

    useEffect(() => {
        if (showSection && !showSectionByDefault) {
            handleScrollToSection();
        }
    }, [showSection, showHideSectionRef]);

    return (
        <Box marginBottom={2}>
            <Box display='flex' flexDirection='row' alignItems='center' marginBottom={2}>
                <div ref={showHideSectionRef} />
                <Typography variant='body1' style={{ fontWeight: 'bold', marginRight: 10 }}>
                    {sectionTitle}
                </Typography>
                {canHideSection && (
                    <Link style={{ cursor: 'pointer' }} onClick={() => setShowSection(visible => !visible)}>
                        { showSection ? "Hide" : "Show" }
                    </Link>
                )}
            </Box>
            {showSection && children}
        </Box>
    );
}