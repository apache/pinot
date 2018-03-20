package com.linkedin.thirdeye.datalayer.dto;

import com.linkedin.thirdeye.datalayer.pojo.EventBean;
import java.util.Objects;


public class EventDTO extends EventBean {

  @Override
  public int hashCode() {
    return com.google.common.base.Objects.hashCode(getName(), getStartTime(), getEndTime(), getEventType());
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof EventDTO)) {
      return false;
    }
    EventDTO eventDto = (EventDTO) obj;
    return Objects.equals(getName(), eventDto.getName()) && Objects.equals(getEventType(), eventDto.getEventType())
        && Objects.equals(getStartTime(), eventDto.getStartTime()) && Objects.equals(getEndTime(),
        eventDto.getEndTime());
  }
}
