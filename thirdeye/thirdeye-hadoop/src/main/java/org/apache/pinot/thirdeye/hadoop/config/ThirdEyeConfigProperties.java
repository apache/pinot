/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.thirdeye.hadoop.config;

/**
 * Class for representing all property names used in thirdeye-hadoop jobs
 */
public enum ThirdEyeConfigProperties {

  /** Pinot table name */
  THIRDEYE_TABLE_NAME("thirdeye.table.name"),

  /** Comma Separated dimension names */
  THIRDEYE_DIMENSION_NAMES("thirdeye.dimension.names"),

  /** Comma Separated dimension types */
  THIRDEYE_DIMENSION_TYPES("thirdeye.dimension.types"),

  /** Comma separated metric names */
  THIRDEYE_METRIC_NAMES("thirdeye.metric.names"),

  /** Comma separated metric types */
  THIRDEYE_METRIC_TYPES("thirdeye.metric.types"),

  /** Time column name */
  THIRDEYE_TIMECOLUMN_NAME("thirdeye.timecolumn.name"),

  /** Time input column type before aggregation (HOURS, DAYS etc) */
  THIRDEYE_INPUT_TIMECOLUMN_TYPE("thirdeye.input.timecolumn.type"),

  /** Time input bucket size before aggregation*/
  THIRDEYE_INPUT_TIMECOLUMN_SIZE("thirdeye.input.timecolumn.size"),

  /** Time format
   * Can be either EPOCH (default) or SIMPLE_DATE_FORMAT:pattern e.g SIMPLE_DATE_FORMAT:yyyyMMdd  */
  THIRDEYE_INPUT_TIMECOLUMN_FORMAT("thirdeye.input.timecolumn.format"),

  /** Time column type (HOURS, DAYS etc) */
  THIRDEYE_TIMECOLUMN_TYPE("thirdeye.timecolumn.type"),

  /** Time bucket size */
  THIRDEYE_TIMECOLUMN_SIZE("thirdeye.timecolumn.size"),

  /** Time format
   * Can be either EPOCH (default) or SIMPLE_DATE_FORMAT:pattern e.g SIMPLE_DATE_FORMAT:yyyyMMdd  */
  THIRDEYE_TIMECOLUMN_FORMAT("thirdeye.timecolumn.format"),

  /** Split threshold for star tree */
  THIRDEYE_SPLIT_THRESHOLD("thirdeye.split.threshold"),

  /** Split order for star tree */
  THIRDEYE_SPLIT_ORDER("thirdeye.split.order"),

  /** Comma separated metric names for threshold filtering */
  THIRDEYE_TOPK_THRESHOLD_METRIC_NAMES("thirdeye.topk.threshold.metric.names"),

  /** Comma separated metric threshold values */
  THIRDEYE_TOPK_METRIC_THRESHOLD_VALUES("thirdeye.topk.metric.threshold.values"),

  /** Comma separated dimension names for topk config */
  THIRDEYE_TOPK_DIMENSION_NAMES("thirdeye.topk.dimension.names"),

  /** Use by appending dimension name at the end eg: thirdeye.topk.metrics.d1
   * Comma separated metrics with topk specification for given dimension */
  THIRDEYE_TOPK_METRICS("thirdeye.topk.metrics"),

  /** Use by appending dimension name at the end eg: thirdeye.topk.kvalues.d1
   * Comma separated top k values for corresponding metrics for given dimension */
  THIRDEYE_TOPK_KVALUES("thirdeye.topk.kvalues"),

  /** Comma separated dimension names which have whitelist */
  THIRDEYE_WHITELIST_DIMENSION_NAMES("thirdeye.whitelist.dimension.names"),

  /** Use by appending dimension name at the end eg: thirdeye.whitelist.dimension.d1
   * Comma separated list of values to whitelist for given dimension */
  THIRDEYE_WHITELIST_DIMENSION("thirdeye.whitelist.dimension"),

  /** Use by appending dimension name at the end eg: thirdeye.nonwhitelist.value.dimension.d1
   * Value to be used for values which don't belong to whitelist */
  THIRDEYE_NONWHITELIST_VALUE_DIMENSION("thirdeye.nonwhitelist.value.dimension");

  String name;

  ThirdEyeConfigProperties(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }

}
