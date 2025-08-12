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

// Standard chart colors matching the ECharts palette
export const CHART_COLORS = [
    "#5470C6", "#91CC75", "#EE6666", "#FAC858", "#73C0DE",
    "#3BA272", "#FC8452", "#9A60B4", "#EA7CCC", "#6E7074",
    "#546570", "#C4CCD3", "#F05B72", "#FF715E", "#FFAF51",
    "#FFE153", "#47B39C", "#5BACE1", "#32C5E9", "#96BFFF"
];

/**
 * Default number of series that can be rendered in the chart
 */
export const DEFAULT_SERIES_LIMIT = 40;

/**
 * Chart padding percentage for time axis and series
 */
export const CHART_PADDING_PERCENTAGE = 0.05; // 5%

/**
 * Get color for a series by index
 * @param index - The series index
 * @returns The color for the series
 */
export const getSeriesColor = (index: number): string => {
  return CHART_COLORS[index % CHART_COLORS.length];
};
