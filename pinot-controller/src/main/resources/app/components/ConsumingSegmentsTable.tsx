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
import { Typography, List, ListItem, ListItemText } from '@material-ui/core';
import CustomizedTables from '../components/Table';
import type { ConsumingSegmentsInfo } from 'Models';

type Props = {
  info: ConsumingSegmentsInfo;
};

const ConsumingSegmentsTable: React.FC<Props> = ({ info }) => {
  const segmentMap = info?._segmentToConsumingInfoMap ?? {};
  const entries = Object.entries(segmentMap);

  if (entries.length === 0) {
    return <Typography>No consuming segment data available.</Typography>;
  }

  const columns = [
    'Segment Name',
    'Server Details',
    'Max Partition Offset Lag',
    'Max Partition Availability Lag (ms)',
  ];

  const records = entries
    .map(([segment, infos]) => {
      const list = infos ?? [];
      if (list.length === 0) {
        return null;
      }
      let segmentMaxOffsetLag = 0;
      let segmentMaxAvailabilityLag = 0;

      const serverDetails = list.map((item) => {
        const lags = Object.values(item.partitionOffsetInfo?.recordsLagMap ?? {});
        const avails = Object.values(item.partitionOffsetInfo?.availabilityLagMsMap ?? {});
        const serverLag = lags
          .filter((lag) => lag != null && Number(lag) > -1)
          .reduce((max, lag) => Math.max(max, Number(lag)), 0);
        const serverAvail = avails
          .filter((av) => av != null && Number(av) > -1)
          .reduce((max, av) => Math.max(max, Number(av)), 0);
        segmentMaxOffsetLag = Math.max(segmentMaxOffsetLag, serverLag);
        segmentMaxAvailabilityLag = Math.max(segmentMaxAvailabilityLag, serverAvail);
        return {
          serverName: item.serverName,
          consumerState: item.consumerState,
          lag: serverLag,
          availabilityMs: serverAvail,
        };
      });
      if (serverDetails.length === 0) {
        return null;
      }
      return [
        segment,
        {
          customRenderer: (
            <List dense disablePadding style={{ width: '100%' }}>
              {serverDetails.map((detail, idx) => (
                <ListItem key={idx} dense disableGutters style={{ padding: '2px 0' }}>
                  <ListItemText
                    primary={`${detail.serverName}: ${detail.consumerState}`}
                    secondary={`Lag: ${detail.lag}, Availability: ${detail.availabilityMs}ms`}
                    primaryTypographyProps={{ variant: 'body2', style: { fontWeight: 500 } }}
                    secondaryTypographyProps={{ variant: 'caption', color: 'textSecondary' }}
                  />
                </ListItem>
              ))}
            </List>
          ),
        },
        segmentMaxOffsetLag,
        segmentMaxAvailabilityLag,
      ];
    })
    .filter(Boolean) as any[];

  if (records.length === 0) {
    return <Typography>Consuming segment data found, but no server details available.</Typography>;
  }

  return (
    <CustomizedTables
      title="Consuming Segments Summary"
      data={{ columns, records }}
      showSearchBox={true}
    />
  );
};

export default ConsumingSegmentsTable;