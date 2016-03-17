package com.linkedin.thirdeye.bootstrap.aggregation;

/**
 *
 *
 */
public enum AggregationJobConstants {

  AGG_INPUT_AVRO_SCHEMA("aggregation.input.avro.schema"), //
  AGG_INPUT_PATH("aggregation.input.path"), //
  AGG_OUTPUT_PATH("aggregation.output.path"), //
  AGG_CONFIG_PATH("aggregation.config.path"),
  AGG_DIMENSION_STATS_PATH("aggregation.dimension.stats.path"),
  AGG_METRIC_SUMS_PATH("aggregation.metric.sums.path"),
  AGG_PRESERVE_TIME_COMPACTION("aggregation.preserve.time.compaction"),
  AGG_CONVERTER_CLASS("aggregation.converter.class"),//
  AGG_JOB_DUMP_STATISTICS("aggregation.job.dump.statistics");//

  String name;

  AggregationJobConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }

}
