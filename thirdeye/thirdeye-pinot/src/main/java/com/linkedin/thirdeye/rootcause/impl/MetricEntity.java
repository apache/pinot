package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.rootcause.Entity;


public class MetricEntity extends Entity {
  public static MetricEntity fromDTO(MetricConfigDTO dto) {
    String urn = String.format("thirdeye:metric:%s:%s", dto.getDataset(), dto.getName());
    return new MetricEntity(urn, dto);
  }

  final MetricConfigDTO dto;

  public MetricEntity(String urn, MetricConfigDTO dto) {
    super(urn);
    this.dto = dto;
  }

  public MetricConfigDTO getDto() {
    return dto;
  }
}
