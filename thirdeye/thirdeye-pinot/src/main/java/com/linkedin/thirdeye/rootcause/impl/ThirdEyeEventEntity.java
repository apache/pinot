package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.anomaly.events.EventType;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.Entity;
import java.util.ArrayList;
import java.util.List;


public class ThirdEyeEventEntity extends EventEntity {

  private final EventDTO dto;

  private ThirdEyeEventEntity(String urn, double score, List<? extends Entity> related, long id, EventDTO dto, String eventType) {
    super(urn, score, related, eventType, id);
    this.dto = dto;
  }

  public EventDTO getDto() {
    return dto;
  }

  @Override
  public ThirdEyeEventEntity withScore(double score) {
    return new ThirdEyeEventEntity(this.getUrn(), score, this.getRelated(), this.getId(), this.dto, getEventType());
  }

  @Override
  public ThirdEyeEventEntity withRelated(List<? extends Entity> related) {
    return new ThirdEyeEventEntity(this.getUrn(), this.getScore(), related, this.getId(), this.dto, getEventType());
  }

  public static ThirdEyeEventEntity fromDTO(double score, EventDTO dto, String eventType) {
    EntityType type = new EntityType(EventEntity.TYPE.getPrefix() + eventType + ":");
    String urn = type.formatURN(dto.getId());
    return new ThirdEyeEventEntity(urn, score, new ArrayList<Entity>(), dto.getId(), dto, eventType);
  }

  public static ThirdEyeEventEntity fromDTO(double score, List<? extends Entity> related, EventDTO dto, String eventType) {
    EntityType type = new EntityType(EventEntity.TYPE.getPrefix() + eventType + ":");
    String urn = type.formatURN(dto.getId());
    return new ThirdEyeEventEntity(urn, score, related, dto.getId(), dto, eventType);
  }
}
