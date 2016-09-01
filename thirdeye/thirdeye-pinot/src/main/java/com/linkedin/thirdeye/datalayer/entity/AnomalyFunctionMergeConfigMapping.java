package com.linkedin.thirdeye.datalayer.entity;

import java.util.List;

public class AnomalyFunctionMergeConfigMapping
    extends AbstractMappingEntity<AnomalyFunction, AnomalyMergeConfig> {

  public AnomalyFunctionMergeConfigMapping(List<Long> anomalyFunctionIds,
      List<Long> anomalyMergeConfigIds) {
    super(anomalyFunctionIds, anomalyMergeConfigIds);
  }
}
