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
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * HolidayEventsPipeline produces EventEntities associated with holidays within the current
 * TimeRange. It matches holidays based on incoming DimensionEntities (e.g. from contribution
 * analysis) and scores them based on the number of matching DimensionEntities.
 * Holiday pipeline will add a buffer of 2 days to the time range provided
 */
public class HolidayEventsPipeline extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(HolidayEventsPipeline.class);

  enum StrategyType {
    LINEAR,
    TRIANGULAR,
    QUADRATIC,
    DIMENSION
  }

  private static final String PROP_K = "k";
  private static final int PROP_K_DEFAULT = -1;

  private static final String PROP_STRATEGY = "strategy";
  private static final String PROP_STRATEGY_DEFAULT = StrategyType.TRIANGULAR.toString();

  private final StrategyType strategy;
  private final EventDataProviderManager eventDataProvider;
  private final int k;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param eventDataProvider event data provider manager
   * @param strategy scoring strategy
   */
  public HolidayEventsPipeline(String outputName, Set<String> inputNames, EventDataProviderManager eventDataProvider, StrategyType strategy, int k) {
    super(outputName, inputNames);
    this.eventDataProvider = eventDataProvider;
    this.strategy = strategy;
    this.k = k;
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
    this.strategy = StrategyType.valueOf(MapUtils.getString(properties, PROP_STRATEGY, PROP_STRATEGY_DEFAULT));
    this.k = MapUtils.getInteger(properties, PROP_K, PROP_K_DEFAULT);
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    TimeRangeEntity anomaly = TimeRangeEntity.getTimeRangeAnomaly(context);
    TimeRangeEntity baseline = TimeRangeEntity.getTimeRangeBaseline(context);
    TimeRangeEntity display = TimeRangeEntity.getTimeRangeDisplay(context);

    ScoringStrategy strategyCurrent = makeStrategy(display.getStart(), anomaly.getStart(), anomaly.getEnd());
    ScoringStrategy strategyBaseline = makeStrategy(baseline.getStart(), baseline.getStart(), baseline.getEnd());

    Set<DimensionEntity> dimensionEntities = context.filter(DimensionEntity.class);
    Map<String, DimensionEntity> urn2entity = EntityUtils.mapEntityURNs(dimensionEntities);

    Set<HolidayEventEntity> entities = new HashSet<>();
    entities.addAll(score(strategyCurrent, this.getHolidayEvents(display.getStart(), anomaly.getEnd(), dimensionEntities), urn2entity, 1.0));
    entities.addAll(score(strategyBaseline, this.getHolidayEvents(baseline.getStart(), baseline.getEnd(), dimensionEntities), urn2entity, baseline.getScore()));

    return new PipelineResult(context, EntityUtils.topk(entities, this.k));
  }

  private List<EventDTO> getHolidayEvents(long start, long end, Set<DimensionEntity> dimensionEntities) {
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

  /* **************************************************************************
   * Entity scoring
   * *************************************************************************/
  private List<HolidayEventEntity> score(ScoringStrategy strategy, Iterable<EventDTO> events, Map<String, DimensionEntity> urn2entity, double coefficient) {
    List<HolidayEventEntity> entities = new ArrayList<>();
    for(EventDTO dto : events) {
      double score = strategy.score(dto, urn2entity) * coefficient;
      entities.add(HolidayEventEntity.fromDTO(score, dto));
    }
    return entities;
  }

  private ScoringStrategy makeStrategy(long lookback, long start, long end) {
    switch(this.strategy) {
      case LINEAR:
        return new ScoreWrapper(new ScoreUtils.LinearStartTimeStrategy(start, end));
      case TRIANGULAR:
        return new ScoreWrapper(new ScoreUtils.TriangularStartTimeStrategy(lookback, start, end));
      case QUADRATIC:
        return new ScoreWrapper(new ScoreUtils.QuadraticTriangularStartTimeStrategy(lookback, start, end));
      case DIMENSION:
        return new DimensionStrategy();
      default:
        throw new IllegalArgumentException(String.format("Invalid strategy type '%s'", this.strategy));
    }
  }

  private interface ScoringStrategy {
    double score(EventDTO dto, Map<String, DimensionEntity> urn2entity);
  }

  private static class ScoreWrapper implements ScoringStrategy {
    private final ScoreUtils.TimeRangeStrategy delegate;

    public ScoreWrapper(ScoreUtils.TimeRangeStrategy delegate) {
      this.delegate = delegate;
    }

    @Override
    public double score(EventDTO dto, Map<String, DimensionEntity> urn2entity) {
      return this.delegate.score(dto.getStartTime(), dto.getEndTime());
    }
  }

  private static class DimensionStrategy implements ScoringStrategy {
    @Override
    public double score(EventDTO dto, Map<String, DimensionEntity> urn2entity) {
      return makeDimensionScore(urn2entity, dto.getTargetDimensionMap());
    }

    private static double makeDimensionScore(Map<String, DimensionEntity> urn2entity, Map<String, List<String>> dimensionFilterMap) {
      double sum = 0.0;
      Set<String> urns = filter2urns(dimensionFilterMap);
      for(String urn : urns) {
        if(urn2entity.containsKey(urn)) {
          sum += urn2entity.get(urn).getScore();
        }
      }
      return sum;
    }

    private static Set<String> filter2urns(Map<String, List<String>> dimensionFilterMap) {
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
