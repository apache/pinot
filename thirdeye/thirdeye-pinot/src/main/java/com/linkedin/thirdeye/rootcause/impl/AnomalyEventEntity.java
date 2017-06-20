package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;


public class AnomalyEventEntity extends EventEntity {
  public static final String EVENT_TYPE_ANOMALY = "anomaly";

  public static final EntityType TYPE = new EntityType(EventEntity.TYPE.getPrefix() + EVENT_TYPE_ANOMALY + ":");

  private final MergedAnomalyResultDTO dto;

  public AnomalyEventEntity(String urn, double score, long id, MergedAnomalyResultDTO dto) {
    super(urn, score, EVENT_TYPE_ANOMALY, id);
    this.dto = dto;
  }

  public MergedAnomalyResultDTO getDto() {
    return dto;
  }

  @Override
  public AnomalyEventEntity withScore(double score) {
    return new AnomalyEventEntity(super.getUrn(), score, super.getId(), this.dto);
  }

  public static AnomalyEventEntity fromDTO(double score, MergedAnomalyResultDTO dto) {
    String urn = TYPE.formatURN(dto.getId());
    return new AnomalyEventEntity(urn, score, dto.getId(), dto);
  }
}
