package com.linkedin.thirdeye.rootcause.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.events.EventFilter;
import com.linkedin.thirdeye.anomaly.events.EventType;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.ExecutionContext;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineResult;

public class HolidayEventsPipeline implements Pipeline {

  private static final Logger LOG = LoggerFactory.getLogger(HolidayEventsPipeline.class);
  private EventManager eventDAO = null;

  public HolidayEventsPipeline(EventManager eventDAO) {
    this.eventDAO = eventDAO;
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public PipelineResult run(ExecutionContext context) {
    TimeRangeEntity timeRangeEntity = EntityUtils.getContextTimeRange(context);
    if(timeRangeEntity == null) {
      LOG.warn("Pipeline '{}' requires TimeRangeEntity. Skipping.", this.getName());
      return new PipelineResult(Collections.<Entity>emptyList());
    }

    EventFilter filter = new EventFilter();
    filter.setStartTime(timeRangeEntity.getStart());
    filter.setEndTime(timeRangeEntity.getEnd());

    // get dimension entity
    // if exists, create map of dimension name to values, and set it in filter

    List<EventDTO> events = getHolidayEvents(filter);

    List<EventEntity> entities = new ArrayList<>();
    for(EventDTO e : events) {
      long distance = timeRangeEntity.getEnd() - e.getStartTime();
      double score = -distance;
      entities.add(EventEntity.fromDTO(score, e));
    }

    return new PipelineResult(entities);
  }

  private List<EventDTO> getHolidayEvents(EventFilter eventFilter) {

    List<EventDTO> allHolidayEventsBetweenTimeRange =
        eventDAO.findEventsBetweenTimeRange(EventType.HOLIDAY.name(), eventFilter.getStartTime(),
            eventFilter.getEndTime());

    Map<String, List<String>> eventFilterDimensionMap = eventFilter.getTargetDimensionMap();
    List<EventDTO> holidayEvents = EventEntity.applyDimensionFilter(allHolidayEventsBetweenTimeRange, eventFilterDimensionMap);
    return holidayEvents;
  }



}
