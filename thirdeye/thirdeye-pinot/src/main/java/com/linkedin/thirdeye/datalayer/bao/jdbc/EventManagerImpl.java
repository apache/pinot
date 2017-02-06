package com.linkedin.thirdeye.datalayer.bao.jdbc;

import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.pojo.EventBean;

public class EventManagerImpl extends AbstractManagerImpl<EventDTO> implements EventManager {
  protected EventManagerImpl() {
    super(EventDTO.class, EventBean.class);
  }

}
