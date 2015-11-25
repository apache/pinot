package com.linkedin.thirdeye.bootstrap.partition;

public enum PartitionPhaseConstants {
  PARTITION_PHASE_INPUT_PATH("partition.phase.input.path"),
  PARTITION_PHASE_OUTPUT_PATH("partition.phase.output.path"),
  PARTITION_PHASE_CONFIG_PATH("partition.phase.config.path"),
  PARTITION_PHASE_NUM_PARTITIONS("partition.phase.num.partitions");

  String name;

  PartitionPhaseConstants(String name) {
    this.name = name;
  }

  public String toString() {
    return name;
  }

}
