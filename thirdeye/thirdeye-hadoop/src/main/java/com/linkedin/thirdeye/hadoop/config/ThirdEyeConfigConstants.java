package com.linkedin.thirdeye.hadoop.config;

/**
 * Class for representing all property names used in thirdeye-hadoop jobs
 */
public enum ThirdEyeConfigConstants {

  /** Pinot table name */
  THIRDEYE_TABLE_NAME("thirdeye.table.name"),

  /** Comma Separated dimension names */
  THIRDEYE_DIMENSION_NAMES("thirdeye.dimension.names"),

  /** Comma separated metric names */
  THIRDEYE_METRIC_NAMES("thirdeye.metric.names"),

  /** Comma separated metric types */
  THIRDEYE_METRIC_TYPES("thirdeye.metric.types"),

  /** Time column name */
  THIRDEYE_TIMECOLUMN_NAME("thirdeye.timecolumn.name"),

  /** Time column type (HOURS, DAYS etc) */
  THIRDEYE_TIMECOLUMN_TYPE("thirdeye.timecolumn.type"),

  /** Time bucket size */
  THIRDEYE_TIMECOLUMN_SIZE("thirdeye.timecolumn.size"),

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
  THIRDEYE_WHITELIST_DIMENSION("thirdeye.whitelist.dimension");

  String name;

  ThirdEyeConfigConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }

}
