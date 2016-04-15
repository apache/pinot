package com.linkedin.thirdeye.hadoop;

public enum ThirdEyeConfigConstants {

  THIRDEYE_TABLE_NAME("thirdeye.table.name"),
  THIRDEYE_DIMENSION_NAMES("thirdeye.dimension.names"),
  THIRDEYE_METRIC_NAMES("thirdeye.metric.names"),
  THIRDEYE_METRIC_TYPES("thirdeye.metric.types"),
  THIRDEYE_TIMECOLUMN_NAME("thirdeye.timecolumn.name"),
  THIRDEYE_TIMECOLUMN_TYPE("thirdeye.timecolumn.type"),
  THIRDEYE_TIMECOLUMN_SIZE("thirdeye.timecolumn.size"),
  THIRDEYE_SPLIT_THRESHOLD("thirdeye.split.threshold"),
  THIRDEYE_SPLIT_ORDER("thirdeye.split.order"),
  THIRDEYE_TOPK_THRESHOLD_METRIC_NAMES("thirdeye.topk.threshold.metric.names"),
  THIRDEYE_TOPK_METRIC_THRESHOLD_VALUES("thirdeye.topk.metric.threshold.values"),
  THIRDEYE_TOPK_DIMENSION_NAMES("thirdeye.topk.dimension.names"),
  THIRDEYE_TOPK_DIMENSION_KVALUES("thirdeye.topk.dimension.kvalues"),
  THIRDEYE_TOPK_DIMENSION_METRICNAMES("thirdeye.topk.dimension.metricnames");

  String name;

  ThirdEyeConfigConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }

}
