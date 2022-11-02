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

import {
  Chip,
  makeStyles,
  Tooltip,
} from "@material-ui/core";
import clsx from "clsx";
import { DISPLAY_SEGMENT_STATUS } from "Models";
import React, { useEffect, useState } from "react";

const useStyles = makeStyles((theme) => ({
  error: {
    color: theme.palette.error.main,
    border: `1px solid ${theme.palette.error.main}`,
  },
  success: {
    color: theme.palette.success.main,
    border: `1px solid ${theme.palette.success.main}`,
  },
  info: {
    color: theme.palette.grey[600],
    border: `1px solid ${theme.palette.grey[600]}`,
  },
  warning: {
    color: theme.palette.warning.main,
    border: `1px solid ${theme.palette.warning.main}`,
  }
}));

interface SegmentStatusRendererProps {
  status: DISPLAY_SEGMENT_STATUS;
}

export enum StatusVariant {
  Error = "error",
  Warning = "warning",
  Info = "info",
  Success = "success",
}

export const SegmentStatusRenderer = ({
  status,
}: SegmentStatusRendererProps) => {
  const [statusTooltipTitle, setStatusTooltipTitle] = useState<string>("");
  const [statusVariant, setStatusVariant] = useState<StatusVariant | null>(
    null
  );
  const segmentStatusRendererClasses = useStyles();

  useEffect(() => {
    initializeValues();
  }, []);

  const initializeValues = () => {
    switch (status) {
      case DISPLAY_SEGMENT_STATUS.GOOD: {
        setStatusVariant(StatusVariant.Success);
        setStatusTooltipTitle("All the servers are ONLINE/CONSUMING");

        break;
      }
      case DISPLAY_SEGMENT_STATUS.BAD: {
        setStatusVariant(StatusVariant.Error);
        setStatusTooltipTitle("One or more servers are in ERROR state");

        break;
      }
      case DISPLAY_SEGMENT_STATUS.PARTIAL: {
        setStatusVariant(StatusVariant.Warning);
        setStatusTooltipTitle("External View is OFFLINE and does not match Ideal State");

        break;
      }
      case DISPLAY_SEGMENT_STATUS.MISSING: {
        setStatusVariant(StatusVariant.Info);
        setStatusTooltipTitle("External View is empty i.e does not have any servers");

        break;
      }
      default: {
        setStatusVariant(StatusVariant.Info);
        setStatusTooltipTitle("Unknown status");
      }
    }
  };

  return (
    <>
      <Tooltip arrow title={statusTooltipTitle} placement="top">
        <Chip
          className={clsx([segmentStatusRendererClasses[statusVariant]])}
          label={status}
          variant="outlined"
        />
      </Tooltip>
    </>
  );
};
