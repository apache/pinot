package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.Entity;
import java.util.ArrayList;
import java.util.List;


public class HolidayEventEntity extends EventEntity {
  public static final String EVENT_TYPE_HOLIDAY = "holiday";

  public static final EntityType TYPE = new EntityType(EventEntity.TYPE.getPrefix() + EVENT_TYPE_HOLIDAY + ":");

  private final EventDTO dto;

  private HolidayEventEntity(String urn, double score, List<? extends Entity> related, long id, EventDTO dto) {
    super(urn, score, related, EVENT_TYPE_HOLIDAY, id);
    this.dto = dto;
  }

  public EventDTO getDto() {
    return dto;
  }

  @Override
  public HolidayEventEntity withScore(double score) {
    return new HolidayEventEntity(this.getUrn(), score, this.getRelated(), this.getId(), this.dto);
  }

  @Override
  public HolidayEventEntity withRelated(List<? extends Entity> related) {
    return new HolidayEventEntity(this.getUrn(), this.getScore(), related, this.getId(), this.dto);
  }

  public static HolidayEventEntity fromDTO(double score, EventDTO dto) {
    String urn = TYPE.formatURN(dto.getId());
    return new HolidayEventEntity(urn, score, new ArrayList<Entity>(), dto.getId(), dto);
  }

  public static HolidayEventEntity fromDTO(double score, List<? extends Entity> related, EventDTO dto) {
    String urn = TYPE.formatURN(dto.getId());
    return new HolidayEventEntity(urn, score, related, dto.getId(), dto);
  }
}
