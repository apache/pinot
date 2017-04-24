package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.anomaly.events.EventDataProviderManager;
import com.linkedin.thirdeye.anomaly.events.EventFilter;
import com.linkedin.thirdeye.anomaly.events.EventType;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HolidayEventsPipeline extends Pipeline {

  private static final Logger LOG = LoggerFactory.getLogger(HolidayEventsPipeline.class);
  final EventDataProviderManager eventDataProvider;

  public HolidayEventsPipeline(String name, Set<String> inputs, EventDataProviderManager eventDataProvider) {
    super(name, inputs);
    this.eventDataProvider = eventDataProvider;
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    TimeRangeEntity current = TimeRangeEntity.getContextCurrent(context);

    EventFilter filter = new EventFilter();
    filter.setEventType(EventType.HOLIDAY.toString());
    filter.setStartTime(current.getStart());
    filter.setEndTime(current.getEnd());
    long duration = current.getEnd() - current.getStart();

    Set<DimensionEntity> dimensionEntities = EntityUtils.filterContext(context, DimensionEntity.class);
    Map<String, DimensionEntity> urn2entity = EntityUtils.mapEntityURNs(dimensionEntities);

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

    Set<EventEntity> entities = new HashSet<>();
    for(EventDTO e : events) {
      long distance = current.getEnd() - e.getStartTime();
      double distanceScore = 1.0 - distance / (double)duration;
      double dimensionScore = makeDimensionScore(urn2entity, e.getTargetDimensionMap());
      EventEntity entity = EventEntity.fromDTO(distanceScore * dimensionScore, e);
      LOG.debug("{}: distance={}, dimension={}, filter={}", entity.getUrn(), distanceScore, dimensionScore, e.getTargetDimensionMap());
      entities.add(entity);
    }

    return new PipelineResult(context, entities);
  }

  static double makeDimensionScore(Map<String, DimensionEntity> urn2entity, Map<String, List<String>> dimensionFilterMap) {
    double sum = 0.0;
    Set<String> urns = filter2urns(dimensionFilterMap);
    for(String urn : urns) {
      if(urn2entity.containsKey(urn)) {
        sum += urn2entity.get(urn).getScore();
      }
    }
    return sum;
  }

  static Set<String> filter2urns(Map<String, List<String>> dimensionFilterMap) {
    Set<String> urns = new HashSet<>();
    for(Map.Entry<String, List<String>> e : dimensionFilterMap.entrySet()) {
      for(String val : e.getValue()) {
        urns.add(DimensionEntity.TYPE.formatURN(e.getKey(), val.toLowerCase()));
      }
    }
    return urns;
  }
}
