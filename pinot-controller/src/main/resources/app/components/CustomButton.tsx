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
import { Button, makeStyles, Tooltip } from '@material-ui/core';

const useStyles = makeStyles((theme) => ({
  button: {
    margin: theme.spacing(1),
    textTransform: 'none'
  }
}));

type Props = {
  children: any;
  onClick: (event: React.MouseEvent<HTMLElement, MouseEvent>) => void,
  isDisabled?: boolean,
  tooltipTitle?: string,
  enableTooltip?: boolean
};

export default function CustomButton({
  children,
  isDisabled,
  onClick,
  tooltipTitle = '',
  enableTooltip = false
}: Props) {
  const classes = useStyles();

  return (
    <Tooltip title={tooltipTitle} disableHoverListener={!enableTooltip} placement="top" arrow>
      <Button
        variant="contained"
        color="primary"
        className={classes.button}
        size="small"
        onClick={onClick}
        disabled={isDisabled}
      >
        {children}
      </Button>
    </Tooltip>
  );
}
