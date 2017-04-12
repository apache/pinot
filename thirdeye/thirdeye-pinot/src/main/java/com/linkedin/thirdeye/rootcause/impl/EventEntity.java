package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.Entity;


public class EventEntity extends Entity {
  public static EventEntity fromDTO(double score, EventDTO dto) {
    String urn = EntityUtils.EntityType.EVENT.formatUrn("%s:%s:%d", dto.getEventType(), dto.getName(), dto.getStartTime());
    return new EventEntity(urn, score, dto);
  }

  final EventDTO dto;

  public EventEntity(String urn, double score, EventDTO dto) {
    super(urn, score);
    this.dto = dto;
  }

  public EventDTO getDto() {
    return dto;
  }
}
