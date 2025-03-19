/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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