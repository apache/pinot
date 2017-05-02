package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.Entity;


/**
 * EventEntity represents an individual event. It holds meta-data referencing ThirdEye's internal
 * database. The URN namespace is defined as 'thirdeye:event:{type}:{name}:{start}'.
 */
public class EventEntity extends Entity {
  public static final EntityType TYPE = new EntityType("thirdeye:event:");

  private final EventDTO dto;

  protected EventEntity(String urn, double score, EventDTO dto) {
    super(urn, score);
    this.dto = dto;
  }

  public EventDTO getDTO() {
    return dto;
  }

  @Override
  public EventEntity withScore(double score) {
    return new EventEntity(this.getUrn(), score, this.dto);
  }

  public static EventEntity fromDTO(double score, EventDTO dto) {
    String urn = TYPE.formatURN(dto.getEventType(), dto.getName(), dto.getStartTime());
    return new EventEntity(urn, score, dto);
  }
}
