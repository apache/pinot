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
 * Holiday pipeline will add a buffer of 2 days to the time range provided
 */
public class HolidayEventsPipeline extends Pipeline {
  private static final int HOLIDAY_DAYS_BUFFER = 2;

  private static final String PROP_STRATEGY = "strategy";

  private static final String STRATEGY_TIME = "time";
  private static final String STRATEGY_DIMENSION = "dimension";

  private static final Logger LOG = LoggerFactory.getLogger(HolidayEventsPipeline.class);

  private final EventDataProviderManager eventDataProvider;

  private final ScoringStrategy strategy;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param eventDataProvider event data provider manager
   * @param strategy scoring strategy
   */
  public HolidayEventsPipeline(String outputName, Set<String> inputNames, EventDataProviderManager eventDataProvider, ScoringStrategy strategy) {
    super(outputName, inputNames);
    this.eventDataProvider = eventDataProvider;
    this.strategy = strategy;
  }

  /**
   * Alternate constructor for RCAFrameworkLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param properties configuration properties ({@code PROP_STRATEGY})
   */
  public HolidayEventsPipeline(String outputName, Set<String> inputNames, Map<String, Object> properties) {
    super(outputName, inputNames);
    this.eventDataProvider = EventDataProviderManager.getInstance();

    String propStrategy = STRATEGY_TIME;
    if(properties.containsKey(PROP_STRATEGY))
      propStrategy = properties.get(PROP_STRATEGY).toString();
    this.strategy = parseStrategy(propStrategy);
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    TimeRangeEntity current = TimeRangeEntity.getContextCurrent(context);
    TimeRangeEntity baseline = TimeRangeEntity.getContextBaseline(context);

    Set<DimensionEntity> dimensionEntities = context.filter(DimensionEntity.class);
    Map<String, DimensionEntity> urn2entity = EntityUtils.mapEntityURNs(dimensionEntities);

    List<EventDTO> events = getHolidayEvents(current, dimensionEntities);

    // TODO evaluate use of baseline events
    //events.addAll(getHolidayEvents(baseline, dimensionEntities));

    long start = new DateTime(current.getStart()).minusDays(HOLIDAY_DAYS_BUFFER).getMillis();
    long end = current.getEnd();

    Set<HolidayEventEntity> entities = new HashSet<>();
    for(EventDTO ev : events) {
      double score = this.strategy.score(ev, start, end, urn2entity);
      HolidayEventEntity entity = HolidayEventEntity.fromDTO(score, ev);
      entities.add(entity);
    }

    return new PipelineResult(context, entities);
  }

  private List<EventDTO> getHolidayEvents(TimeRangeEntity timerangeEntity, Set<DimensionEntity> dimensionEntities) {
    long start = new DateTime(timerangeEntity.getStart()).minusDays(HOLIDAY_DAYS_BUFFER).getMillis();
    long end = timerangeEntity.getEnd();

    EventFilter filter = new EventFilter();
    filter.setEventType(EventType.HOLIDAY.toString());
    filter.setStartTime(start);
    filter.setEndTime(end);

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

    return eventDataProvider.getEvents(filter);
  }

  private static ScoringStrategy parseStrategy(String strategy) {
    switch(strategy) {
      case STRATEGY_TIME:
        return new TimeStrategy();
      case STRATEGY_DIMENSION:
        return new DimensionStrategy();
    }
    throw new IllegalArgumentException(String.format("Unknown strategy '%s'", strategy));
  }

  private interface ScoringStrategy {
    double score(EventDTO dto, long start, long end, Map<String, DimensionEntity> urn2entity);
  }

  private static class TimeStrategy implements ScoringStrategy {
    @Override
    public double score(EventDTO dto, long start, long end, Map<String, DimensionEntity> urn2entity) {
      long duration = end - start;
      return (dto.getStartTime() - start) / (double) duration;
    }
  }

  private static class DimensionStrategy implements ScoringStrategy {
    @Override
    public double score(EventDTO dto, long start, long end, Map<String, DimensionEntity> urn2entity) {
      return makeDimensionScore(urn2entity, dto.getTargetDimensionMap());
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
}
