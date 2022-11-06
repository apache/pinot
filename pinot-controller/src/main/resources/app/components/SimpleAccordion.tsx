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

import React from 'react';
import { Theme, createStyles, makeStyles } from '@material-ui/core/styles';
import Accordion from '@material-ui/core/Accordion';
import AccordionSummary from '@material-ui/core/AccordionSummary';
import AccordionDetails from '@material-ui/core/AccordionDetails';
import Typography from '@material-ui/core/Typography';
import ExpandMoreIcon from '@material-ui/icons/ExpandMore';
import SearchBar from './SearchBar';
import { FormControlLabel, Switch, Tooltip } from '@material-ui/core';
import clsx from 'clsx';

const useStyles = makeStyles((theme: Theme) =>
  createStyles({
    root: {
      backgroundColor: 'rgba(66, 133, 244, 0.1)',
      borderBottom: '1px #BDCCD9 solid',
      minHeight: '0 !important',
      '& .MuiAccordionSummary-content.Mui-expanded':{
        margin: 0,
        alignItems: 'center',
      }
    },
    heading: {
      fontWeight: 600,
      letterSpacing: '1px',
      fontSize: '1rem',
      color: '#4285f4'
    },
    details: {
      flexDirection: 'column',
      padding: '0',
      overflow: "auto"
    },
    formControl: {
      marginRight: 0,
      marginLeft: 'auto',
      zoom: 0.85
    }
  }),
);

type Props = {
  headerTitle: string;
  tooltipContent?: any;
  showSearchBox: boolean;
  searchValue?: string;
  handleSearch?: Function;
  recordCount?: number;
  children: any;
  accordionToggleObject?: {
    toggleChangeHandler: (event: React.ChangeEvent<HTMLInputElement>) => void;
    toggleName: string;
    toggleValue: boolean;
  },
  detailsContainerClass?: string
};

export default function SimpleAccordion({
  headerTitle,
  tooltipContent,
  showSearchBox,
  searchValue,
  handleSearch,
  recordCount,
  children,
  accordionToggleObject,
  detailsContainerClass
}: Props) {
  const classes = useStyles();

  return (
    <Accordion
      defaultExpanded={true}
    >
      <AccordionSummary
        expandIcon={<ExpandMoreIcon />}
        aria-controls={`panel1a-content-${headerTitle}`}
        id={`panel1a-header-${headerTitle}`}
        className={classes.root}
      >
        {tooltipContent ?
          <Tooltip interactive title={tooltipContent} arrow placement="top">
            <Typography className={classes.heading}>{`${headerTitle.toUpperCase()} ${recordCount !== undefined ? ` - (${recordCount})` : ''}`}</Typography>
          </Tooltip>
        :
          <Typography className={classes.heading}>{`${headerTitle.toUpperCase()} ${recordCount !== undefined ? ` - (${recordCount})` : ''}`}</Typography>
        }
        {accordionToggleObject &&
          <FormControlLabel
            className={classes.formControl}
            control={
              <Switch
                checked={accordionToggleObject.toggleValue}
                onChange={accordionToggleObject.toggleChangeHandler} 
                name={accordionToggleObject.toggleName}
                color="primary"
              />
            }
            label={accordionToggleObject.toggleName}
          />
        }
      </AccordionSummary>
      <AccordionDetails className={clsx(classes.details, detailsContainerClass)}>
        {showSearchBox ?
          <SearchBar
            // searchOnRight={true}
            value={searchValue}
            onChange={(e) => handleSearch(e.target.value)}
          />
          : null}
        {children}
      </AccordionDetails>
    </Accordion>
  );
}