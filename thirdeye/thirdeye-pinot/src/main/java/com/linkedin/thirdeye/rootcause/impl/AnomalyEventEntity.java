package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.rootcause.Entity;
import java.util.ArrayList;
import java.util.List;


public class AnomalyEventEntity extends EventEntity {
  public static final String EVENT_TYPE_ANOMALY = "anomaly";

  public static final EntityType TYPE = new EntityType(EventEntity.TYPE.getPrefix() + EVENT_TYPE_ANOMALY + ":");

  protected AnomalyEventEntity(String urn, double score, List<? extends Entity> related, long id) {
    super(urn, score, related, EVENT_TYPE_ANOMALY, id);
  }

  @Override
  public AnomalyEventEntity withScore(double score) {
    return new AnomalyEventEntity(this.getUrn(), score, this.getRelated(), this.getId());
  }

  @Override
  public AnomalyEventEntity withRelated(List<? extends Entity> related) {
    return new AnomalyEventEntity(this.getUrn(), this.getScore(), related, this.getId());
  }

  public static AnomalyEventEntity fromDTO(double score, MergedAnomalyResultDTO dto) {
    String urn = TYPE.formatURN(dto.getId());
    return new AnomalyEventEntity(urn, score, new ArrayList<Entity>(), dto.getId());
  }

  public static AnomalyEventEntity fromURN(String urn, double score) {
    if(!TYPE.isType(urn))
      throw new IllegalArgumentException(String.format("URN '%s' is not type '%s'", urn, TYPE.getPrefix()));
    String[] parts = urn.split(":", 4);
    if(parts.length != 4)
      throw new IllegalArgumentException(String.format("URN must have 4 parts but has '%d'", parts.length));
    return new AnomalyEventEntity(urn, score, new ArrayList<Entity>(), Long.parseLong(parts[3]));
  }
}
