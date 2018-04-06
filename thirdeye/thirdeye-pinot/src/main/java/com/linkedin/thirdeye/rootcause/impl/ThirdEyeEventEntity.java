package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.anomaly.events.EventType;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.Entity;
import java.util.ArrayList;
import java.util.List;


public class ThirdEyeEventEntity extends EventEntity {

  private final EventDTO dto;
  private EventType eventType;

  private ThirdEyeEventEntity(String urn, double score, List<? extends Entity> related, long id, EventDTO dto, EventType eventType) {
    super(urn, score, related, eventType.toString(), id);
    this.eventType = eventType;
    this.dto = dto;
  }

  @Override
  public String getEventType() {
    return eventType.toString();
  }

  public EventDTO getDto() {
    return dto;
  }

  @Override
  public ThirdEyeEventEntity withScore(double score) {
    return new ThirdEyeEventEntity(this.getUrn(), score, this.getRelated(), this.getId(), this.dto, this.eventType);
  }

  @Override
  public ThirdEyeEventEntity withRelated(List<? extends Entity> related) {
    return new ThirdEyeEventEntity(this.getUrn(), this.getScore(), related, this.getId(), this.dto, this.eventType);
  }

  public static ThirdEyeEventEntity fromDTO(double score, EventDTO dto, EventType eventType) {
    EntityType type = new EntityType(EventEntity.TYPE.getPrefix() + eventType + ":");
    String urn = type.formatURN(dto.getId());
    return new ThirdEyeEventEntity(urn, score, new ArrayList<Entity>(), dto.getId(), dto, eventType);
  }

  public static ThirdEyeEventEntity fromDTO(double score, List<? extends Entity> related, EventDTO dto, EventType eventType) {
    EntityType type = new EntityType(EventEntity.TYPE.getPrefix() + eventType + ":");
    String urn = type.formatURN(dto.getId());
    return new ThirdEyeEventEntity(urn, score, related, dto.getId(), dto, eventType);
  }
}
