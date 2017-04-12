package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.Entity;


public class EventEntityUtils {
  public static Entity entityFromDTO(EventDTO dto) {
    String urn = String.format("thirdeye:event:%s:%s:%d", dto.getEventType(), dto.getName(), dto.getStartTime());
    return Entity.fromURN(urn);
  }
}
