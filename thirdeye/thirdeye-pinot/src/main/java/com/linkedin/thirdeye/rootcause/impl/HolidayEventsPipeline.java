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
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * HolidayEventsPipeline produces EventEntities associated with holidays within the current
 * TimeRange. It matches holidays based on incoming DimensionEntities (e.g. from contribution
 * analysis) and scores them based on the number of matching DimensionEntities.
 */
public class HolidayEventsPipeline extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(HolidayEventsPipeline.class);

  private final EventDataProviderManager eventDataProvider;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param eventDataProvider event data provider manager
   */
  public HolidayEventsPipeline(String outputName, Set<String> inputNames, EventDataProviderManager eventDataProvider) {
    super(outputName, inputNames);
    this.eventDataProvider = eventDataProvider;
  }

  /**
   * Alternate constructor for PipelineLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param ignore configuration properties (none)
   */
  public HolidayEventsPipeline(String outputName, Set<String> inputNames, Map<String, String> ignore) {
    super(outputName, inputNames);
    this.eventDataProvider = EventDataProviderManager.getInstance();
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    TimeRangeEntity current = TimeRangeEntity.getContextCurrent(context);
    long currentStart = new DateTime(current.getStart()).minusDays(2).getMillis();
    long currentEnd = current.getEnd();
    long baselineStart = new DateTime(currentStart).minusDays(7).getMillis();
    long baselineEnd = new DateTime(currentEnd).minusDays(7).getMillis();
    TimeRangeEntity baseline = TimeRangeEntity.fromRange(1.0, "baseline", baselineStart, baselineEnd);

    Set<DimensionEntity> dimensionEntities = context.filter(DimensionEntity.class);
    Map<String, DimensionEntity> urn2entity = EntityUtils.mapEntityURNs(dimensionEntities);

    List<EventDTO> events = getHolidayEvents(current, dimensionEntities);
    events.addAll(getHolidayEvents(baseline, dimensionEntities));

    Set<EventEntity> entities = new HashSet<>();
    for(EventDTO ev : events) {
      long distance = current.getEnd() - ev.getStartTime();
      double dimensionScore = makeDimensionScore(urn2entity, ev.getTargetDimensionMap());
      EventEntity entity = EventEntity.fromDTO(dimensionScore, ev);
      LOG.debug("{}: dimension={}, filter={}", entity.getUrn(), dimensionScore, ev.getTargetDimensionMap());
      entities.add(entity);
    }

    return new PipelineResult(context, entities);
  }

  private List<EventDTO> getHolidayEvents(TimeRangeEntity timerangeEntity, Set<DimensionEntity> dimensionEntities) {
    EventFilter filter = new EventFilter();
    filter.setEventType(EventType.HOLIDAY.toString());
    filter.setStartTime(timerangeEntity.getStart());
    filter.setEndTime(timerangeEntity.getEnd());

    Map<String, List<String>> filterMap = new HashMap<>();
    if (CollectionUtils.isNotEmpty(dimensionEntities)) {
      for (DimensionEntity dimensionEntity : dimensionEntities) {
        String dimensionName = dimensionEntity.getName();
        String dimensionValue = dimensionEntity.getValue();
        if (!filterMap.containsKey(dimensionName)) {
          filterMap.put(dimensionName, new ArrayList<String>());
        }
        filterMap.get(dimensionName).add(dimensionValue);
      }
    }
    filter.setTargetDimensionMap(filterMap);

    List<EventDTO> events = eventDataProvider.getEvents(filter);
    return events;
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
