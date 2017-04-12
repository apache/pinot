package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.Metadata;


public class EventMetadata implements Metadata {
  EventDTO event;

  public EventMetadata(EventDTO event) {
    this.event = event;
  }

  public EventDTO getEvent() {
    return event;
  }

  public void setEvent(EventDTO event) {
    this.event = event;
  }
}
