package com.linkedin.thirdeye.dbi.entity.mapping;

import com.linkedin.thirdeye.dbi.entity.base.AbstractMappingEntity;
import com.linkedin.thirdeye.dbi.entity.data.AnomalyFunction;
import com.linkedin.thirdeye.dbi.entity.data.AnomalyMergeConfig;
import java.util.List;

public class AnomalyFunctionMergeConfig
    extends AbstractMappingEntity<AnomalyFunction, AnomalyMergeConfig> {

  public AnomalyFunctionMergeConfig(List<Long> anomalyFunctionIds,
      List<Long> anomalyMergeConfigIds) {
    super(anomalyFunctionIds, anomalyMergeConfigIds);
  }
}
