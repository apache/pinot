package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.rootcause.Entity;
import java.util.ArrayList;
import java.util.List;


public class AnomalyEventEntity extends EventEntity {
  public static final String EVENT_TYPE_ANOMALY = "anomaly";

  public static final EntityType TYPE = new EntityType(EventEntity.TYPE.getPrefix() + EVENT_TYPE_ANOMALY + ":");

  private final MergedAnomalyResultDTO dto;

  protected AnomalyEventEntity(String urn, double score, List<? extends Entity> related, long id, MergedAnomalyResultDTO dto) {
    super(urn, score, related, EVENT_TYPE_ANOMALY, id);
    this.dto = dto;
  }

  public MergedAnomalyResultDTO getDto() {
    return dto;
  }

  @Override
  public AnomalyEventEntity withScore(double score) {
    return new AnomalyEventEntity(this.getUrn(), score, this.getRelated(), this.getId(), this.dto);
  }

  @Override
  public AnomalyEventEntity withRelated(List<? extends Entity> related) {
    return new AnomalyEventEntity(this.getUrn(), this.getScore(), related, this.getId(), this.dto);
  }

  public static AnomalyEventEntity fromDTO(double score, MergedAnomalyResultDTO dto) {
    String urn = TYPE.formatURN(dto.getId());
    return new AnomalyEventEntity(urn, score, new ArrayList<Entity>(), dto.getId(), dto);
  }
}
