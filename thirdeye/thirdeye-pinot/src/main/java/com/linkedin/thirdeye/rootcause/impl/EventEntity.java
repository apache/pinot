package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.Entity;


public class EventEntity extends Entity {
  public static EventEntity fromDTO(EventDTO dto) {
    String urn = String.format("thirdeye:event:%s:%s:%d", dto.getEventType(), dto.getName(), dto.getStartTime());
    return new EventEntity(urn, dto);
  }

  final EventDTO dto;

  public EventEntity(String urn, EventDTO dto) {
    super(urn);
    this.dto = dto;
  }

  public EventDTO getDto() {
    return dto;
  }
}
