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

import React, { useMemo } from 'react';
import ReactECharts from 'echarts-for-react';
import { makeStyles } from '@material-ui/core/styles';
import { Typography, Paper } from '@material-ui/core';
import { ChartSeries } from 'Models';
import { getSeriesColor, CHART_PADDING_PERCENTAGE } from '../../utils/ChartConstants';

// Define proper types for ECharts parameters
interface EChartsTooltipParams {
  value: [number, number];
  seriesName: string;
  color: string;
}

// Extract chart configuration functions
const createChartSeries = (series: ChartSeries[], selectedMetric?: string) => {
  return series.map((s, index) => {
    const isSelected = selectedMetric ? s.name === selectedMetric : true;

    return {
      name: s.name,
      type: 'line',
      data: s.data.map(dp => [dp.timestamp, dp.value]),
      smooth: false,
      symbol: 'circle',
      symbolSize: isSelected ? 8 : 4,
      lineStyle: {
        width: 1,
        color: getSeriesColor(index),
        opacity: isSelected ? 1 : 0,
      },
      itemStyle: {
        color: getSeriesColor(index),
        opacity: isSelected ? 1 : 0,
      },
      emphasis: {
        focus: 'none',
        scale: false,
      },
    };
  });
};

const createTooltipFormatter = (selectedMetric?: string) => {
  return function (params: EChartsTooltipParams[]) {
    let result = `<div style="font-weight: bold;">${new Date(params[0].value[0]).toLocaleString()}</div>`;

    // Filter params to only show selected series when one is selected
    const filteredParams = selectedMetric
      ? params.filter((param: EChartsTooltipParams) => param.seriesName === selectedMetric)
      : params;

    filteredParams.forEach((param: EChartsTooltipParams) => {
      const color = param.color;
      const name = param.seriesName;
      const value = param.value[1];
      result += `<div style="margin: 5px 0;">
        <span style="display: inline-block; width: 10px; height: 10px; background: ${color}; margin-right: 5px;"></span>
        <span style="font-weight: bold;">${name}:</span> ${value}
      </div>`;
    });
    return result;
  };
};

const getTimeRange = (series: ChartSeries[]) => {
  const allTimestamps = series.flatMap(s => s.data.map(dp => dp.timestamp));
  const minTime = Math.min(...allTimestamps);
  const maxTime = Math.max(...allTimestamps);
  return { minTime, maxTime };
};

const useStyles = makeStyles((theme) => ({
  chartContainer: {
    height: '500px',
    width: '100%',
    padding: theme.spacing(2),
  },
  noDataMessage: {
    display: 'flex',
    justifyContent: 'center',
    alignItems: 'center',
    height: '200px',
    color: theme.palette.text.secondary,
  },
  chartTitle: {
    marginBottom: theme.spacing(2),
    fontWeight: 'bold',
  },
}));

interface TimeseriesChartProps {
  series: ChartSeries[];
  height?: number;
  selectedMetric?: string;
}

const TimeseriesChart: React.FC<TimeseriesChartProps> = ({
  series,
  height = 500,
  selectedMetric
}) => {
  const classes = useStyles();

  const chartOption = useMemo(() => {
    if (!series || series.length === 0) {
      return {};
    }

    const chartSeries = createChartSeries(series, selectedMetric);
    const { minTime, maxTime } = getTimeRange(series);
    const timeRange = maxTime - minTime;
    const padding = timeRange * CHART_PADDING_PERCENTAGE;

    return {
      tooltip: {
        trigger: 'axis',
        formatter: createTooltipFormatter(selectedMetric),
        axisPointer: {
          type: 'cross',
          label: {
            backgroundColor: '#6a7985',
          },
        },
        // Fix hover popup positioning to keep it within chart area
        confine: true,
        position: function (point: [number, number], params: unknown, dom: HTMLElement, rect: { x: number; y: number; width: number; height: number }, size: { viewSize: [number, number]; contentSize: [number, number] }) {
          // Ensure tooltip stays within chart bounds
          const [x, y] = point;
          const [viewWidth, viewHeight] = size.viewSize;
          const [contentWidth, contentHeight] = size.contentSize;

          let posX = x + 10;
          let posY = y - 10;

          // Adjust horizontal position if tooltip would go outside
          if (posX + contentWidth > viewWidth) {
            posX = x - contentWidth - 10;
          }

          // Adjust vertical position if tooltip would go outside
          if (posY - contentHeight < 0) {
            posY = y + 10;
          }

          return [posX, posY];
        },
      },
      // Remove legend - colors will be shown in the stats table instead
      grid: {
        left: '3%',
        right: '4%',
        bottom: '3%',
        top: '15%',
        containLabel: true,
      },
      xAxis: {
        type: 'time',
        boundaryGap: false,
        min: minTime - padding, // Apply consistent padding
        max: maxTime + padding, // Apply consistent padding
        axisLabel: {
          formatter: function (value: number) {
            return new Date(value).toLocaleTimeString();
          },
        },
      },
      yAxis: {
        type: 'value',
        axisLabel: {
          formatter: function (value: number) {
            return value.toFixed(2);
          },
        },
      },
      dataZoom: [
        {
          type: 'inside',
          start: 0,
          end: 100,
          zoomOnMouseWheel: true,
          moveOnMouseMove: true,
          moveOnMouseWheel: false,
          preventDefaultMouseMove: false,
          filterMode: 'filter',
          rangeMode: ['value', 'value'],
        },
      ],
      toolbox: {
        feature: {
          dataZoom: {
            yAxisIndex: 'none',
            title: {
              zoom: 'Zoom Selection'
            }
          },
        },
        right: 20,
        top: 20,
      },
      series: chartSeries,
    };
  }, [series, selectedMetric]);

  if (!series || series.length === 0) {
    return (
      <Paper className={classes.chartContainer}>
        <div className={classes.noDataMessage}>
          <Typography variant="h6">No timeseries data available</Typography>
        </div>
      </Paper>
    );
  }

  return (
    <Paper className={classes.chartContainer} style={{ height: `${height}px` }}>
      <ReactECharts
        option={chartOption}
        style={{ height: '100%', width: '100%' }}
        opts={{ renderer: 'canvas' }}
      />
    </Paper>
  );
};

export default TimeseriesChart;
