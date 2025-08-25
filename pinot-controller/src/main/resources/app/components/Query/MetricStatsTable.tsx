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
import {
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Typography,
  makeStyles,
} from '@material-ui/core';

import { ChartSeries } from 'Models';
import { getSeriesColor } from '../../utils/ChartConstants';

const useStyles = makeStyles((theme) => ({
  tableContainer: {
    marginTop: theme.spacing(2),
    maxHeight: 400,
  },
  table: {
    minWidth: 650,
    tableLayout: 'fixed',
  },
  tableHead: {
    backgroundColor: theme.palette.grey[100],
  },
  tableHeadCell: {
    color: theme.palette.text.primary,
    fontSize: '0.875rem',
  },
  tableRow: {
    cursor: 'pointer',
    '&:hover': {
      backgroundColor: theme.palette.action.hover,
    },
    '&.selected': {
      backgroundColor: theme.palette.primary.light,
      color: theme.palette.primary.contrastText,
    },
    '&.dimmed': {
      opacity: 0.4,
      backgroundColor: theme.palette.grey[50],
    },
  },
  metricCell: {
    maxWidth: 300,
    wordBreak: 'break-word',
    fontSize: '0.875rem',
  },
  statCell: {
    fontSize: '0.875rem',
  },
  colorBox: {
    width: 10,
    height: 10,
    borderRadius: 2,
    flexShrink: 0,
    margin: '0 auto',
  },

  noDataMessage: {
    textAlign: 'center',
    padding: theme.spacing(3),
    color: theme.palette.text.secondary,
  },
  tableTitle: {
    marginBottom: theme.spacing(1),
    fontWeight: 'bold',
    fontSize: '1.1rem',
  },
}));

interface MetricStatsTableProps {
  series: ChartSeries[];
  selectedMetric?: string;
  onMetricSelect?: (metricName: string | null) => void;
}

const MetricStatsTable: React.FC<MetricStatsTableProps> = ({
  series,
  selectedMetric,
  onMetricSelect
}) => {
  const classes = useStyles();

  const formatValue = (value: number): string => {
    if (Math.abs(value) >= 1e6) {
      return (value / 1e6).toFixed(2) + 'M';
    } else if (Math.abs(value) >= 1e3) {
      return (value / 1e3).toFixed(2) + 'K';
    } else {
      return value.toFixed(2);
    }
  };


  const handleRowClick = (metricName: string) => {
    if (onMetricSelect) {
      // If clicking the same metric, deselect it
      if (selectedMetric === metricName) {
        onMetricSelect(null);
      } else {
        onMetricSelect(metricName);
      }
    }
  };

  const isRowSelected = (metricName: string) => selectedMetric === metricName;
  const isRowDimmed = (metricName: string) => selectedMetric && selectedMetric !== metricName;

  if (!series || series.length === 0) {
    return (
      <div className={classes.noDataMessage}>
        <Typography variant="h6">No metric data available</Typography>
        <Typography variant="body2">Run a timeseries query to see statistics</Typography>
      </div>
    );
  }

  return (
    <div>

      <TableContainer component={Paper} className={classes.tableContainer}>
        <Table className={classes.table} size="small">
          <TableHead className={classes.tableHead}>
            <TableRow>
              <TableCell className={classes.tableHeadCell} align="center" style={{ width: '40px', padding: '8px 4px' }}>Color</TableCell>
              <TableCell className={classes.tableHeadCell}>Metric</TableCell>
              <TableCell className={classes.tableHeadCell} align="right">Count</TableCell>
              <TableCell className={classes.tableHeadCell} align="right">Min</TableCell>
              <TableCell className={classes.tableHeadCell} align="right">Max</TableCell>
              <TableCell className={classes.tableHeadCell} align="right">Average</TableCell>
              <TableCell className={classes.tableHeadCell} align="right">Sum</TableCell>
              <TableCell className={classes.tableHeadCell} align="right">First Value</TableCell>
              <TableCell className={classes.tableHeadCell} align="right">Last Value</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {series.map((chartSeries) => {
              const { name, stats } = chartSeries;
              const isSelected = isRowSelected(name);
              const isDimmed = isRowDimmed(name);

              return (
                <TableRow
                  key={name}
                  className={`${classes.tableRow} ${isSelected ? 'selected' : ''} ${isDimmed ? 'dimmed' : ''}`}
                  onClick={() => handleRowClick(name)}
                >
                  <TableCell align="center" style={{ width: '40px', padding: '8px 4px' }}>
                    <div
                      className={classes.colorBox}
                      style={{ backgroundColor: getSeriesColor(series.findIndex(s => s.name === name)) }}
                    />
                  </TableCell>
                  <TableCell className={classes.metricCell}>
                    {name}
                  </TableCell>
                  <TableCell className={classes.statCell} align="right">
                    {stats.count}
                  </TableCell>
                  <TableCell className={classes.statCell} align="right">
                    {formatValue(stats.min)}
                  </TableCell>
                  <TableCell className={classes.statCell} align="right">
                    {formatValue(stats.max)}
                  </TableCell>
                  <TableCell className={classes.statCell} align="right">
                    {formatValue(stats.avg)}
                  </TableCell>
                  <TableCell className={classes.statCell} align="right">
                    {formatValue(stats.sum)}
                  </TableCell>
                  <TableCell className={classes.statCell} align="right">
                    {formatValue(stats.firstValue)}
                  </TableCell>
                  <TableCell className={classes.statCell} align="right">
                    {formatValue(stats.lastValue)}
                  </TableCell>
                </TableRow>
              );
            })}
          </TableBody>
        </Table>
      </TableContainer>
    </div>
  );
};

export default MetricStatsTable;
