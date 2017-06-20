package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.dto.EventDTO;


public class HolidayEventEntity extends EventEntity {
  public static final String EVENT_TYPE_HOLIDAY = "holiday";

  public static final EntityType TYPE = new EntityType(EventEntity.TYPE.getPrefix() + EVENT_TYPE_HOLIDAY + ":");

  private final EventDTO dto;

  private HolidayEventEntity(String urn, double score, long id, EventDTO dto) {
    super(urn, score, EVENT_TYPE_HOLIDAY, id);
    this.dto = dto;
  }

  public EventDTO getDto() {
    return dto;
  }

  @Override
  public HolidayEventEntity withScore(double score) {
    return new HolidayEventEntity(super.getUrn(), score, super.getId(), this.dto);
  }

  public static HolidayEventEntity fromDTO(double score, EventDTO dto) {
    String urn = TYPE.formatURN(dto.getId());
    return new HolidayEventEntity(urn, score, dto.getId(), dto);
  }
}
