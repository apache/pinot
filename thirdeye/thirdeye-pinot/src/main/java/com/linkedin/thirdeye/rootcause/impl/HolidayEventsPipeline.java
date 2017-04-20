package com.linkedin.thirdeye.rootcause.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.thirdeye.anomaly.events.EventDataProviderManager;
import com.linkedin.thirdeye.anomaly.events.EventFilter;
import com.linkedin.thirdeye.anomaly.events.EventType;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.ExecutionContext;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineResult;

public class HolidayEventsPipeline implements Pipeline {

  private static final Logger LOG = LoggerFactory.getLogger(HolidayEventsPipeline.class);
  private EventDataProviderManager eventDataProvider = null;

  public HolidayEventsPipeline(EventDataProviderManager eventDataProvider) {
    this.eventDataProvider = eventDataProvider;
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public PipelineResult run(ExecutionContext context) {
    TimeRangeEntity timeRangeEntity = TimeRangeEntity.getContextCurrent(context);

    EventFilter filter = new EventFilter();
    filter.setEventType(EventType.HOLIDAY);
    filter.setStartTime(timeRangeEntity.getStart());
    filter.setEndTime(timeRangeEntity.getEnd());

    Set<DimensionEntity> dimensionEntities = EntityUtils.filterContext(context, DimensionEntity.class);
    Map<String, List<String>> dimensionFilterMap = new HashMap<>();
    if (CollectionUtils.isNotEmpty(dimensionEntities)) {
      for (DimensionEntity dimensionEntity : dimensionEntities) {
        String dimensionName = dimensionEntity.getName();
        String dimensionValue = dimensionEntity.getValue();
        if (!dimensionFilterMap.containsKey(dimensionName)) {
          dimensionFilterMap.put(dimensionName, new ArrayList<String>());
        }
        dimensionFilterMap.get(dimensionName).add(dimensionValue);
      }
    }
    filter.setTargetDimensionMap(dimensionFilterMap);

    List<EventDTO> events = eventDataProvider.getEvents(filter);

    List<EventEntity> entities = new ArrayList<>();
    for(EventDTO e : events) {
      long distance = timeRangeEntity.getEnd() - e.getStartTime();
      double score = -distance;
      entities.add(EventEntity.fromDTO(score, e));
    }

    return new PipelineResult(entities);
  }



}
