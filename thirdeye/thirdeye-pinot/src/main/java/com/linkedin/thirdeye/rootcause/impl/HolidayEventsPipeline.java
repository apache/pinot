package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.anomaly.events.EventDataProviderManager;
import com.linkedin.thirdeye.anomaly.events.EventFilter;
import com.linkedin.thirdeye.anomaly.events.EventType;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.MaxScoreSet;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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

  private static final String DIMENSION_COUNTRY_CODE = "countryCode";

  enum StrategyType {
    LINEAR,
    TRIANGULAR,
    QUADRATIC,
    HYPERBOLA,
    DIMENSION,
    COMPOUND
  }

  private static final String PROP_K = "k";
  private static final int PROP_K_DEFAULT = -1;

  private static final String PROP_STRATEGY = "strategy";
  private static final String PROP_STRATEGY_DEFAULT = StrategyType.COMPOUND.toString();

  private static final long OVERFETCH = TimeUnit.DAYS.toMillis(1);

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
    TimeRangeEntity analysis = TimeRangeEntity.getTimeRangeAnalysis(context);

    ScoringStrategy strategyAnomaly = makeStrategy(analysis.getStart(), anomaly.getStart(), anomaly.getEnd());
    ScoringStrategy strategyBaseline = makeStrategy(baseline.getStart(), baseline.getStart(), baseline.getEnd());

    // use both provided and generated
    Set<DimensionEntity> dimensionEntities = context.filter(DimensionEntity.class);
    Map<String, DimensionEntity> countryCodeLookup = new HashMap<>();
    for (DimensionEntity dimension : dimensionEntities) {
      if (dimension.getName().equals(DIMENSION_COUNTRY_CODE)) {
        countryCodeLookup.put(dimension.getValue(), dimension);
      }
    }

    Set<HolidayEventEntity> entities = new MaxScoreSet<>();
    entities.addAll(EntityUtils.addRelated(score(strategyAnomaly,
        this.getHolidayEvents(analysis.getStart(), anomaly.getEnd()), countryCodeLookup, anomaly.getScore()), anomaly));
    entities.addAll(EntityUtils.addRelated(score(strategyBaseline,
        this.getHolidayEvents(baseline.getStart(), baseline.getEnd()), countryCodeLookup, baseline.getScore()), baseline));

    return new PipelineResult(context, EntityUtils.topk(entities, this.k));
  }

  private List<EventDTO> getHolidayEvents(long start, long end) {
    EventFilter filter = new EventFilter();
    filter.setEventType(EventType.HOLIDAY.toString());
    // overfetch to allow for timezone discrepancies
    filter.setStartTime(start - OVERFETCH);
    filter.setEndTime(end + OVERFETCH);
    return eventDataProvider.getEvents(filter);
  }

  /* **************************************************************************
   * Entity scoring
   * *************************************************************************/
  private List<HolidayEventEntity> score(ScoringStrategy strategy, Iterable<EventDTO> events, Map<String, DimensionEntity> countryCodeLookup, double coefficient) {
    List<HolidayEventEntity> entities = new ArrayList<>();
    for(EventDTO dto : events) {
      List<Entity> related = new ArrayList<>();

      if (dto.getTargetDimensionMap().containsKey(DIMENSION_COUNTRY_CODE)) {
        for (String countryCode : dto.getTargetDimensionMap().get(DIMENSION_COUNTRY_CODE)) {
          final String countryKey = countryCode.toLowerCase();
          if (countryCodeLookup.containsKey(countryKey)) {
            related.add(countryCodeLookup.get(countryKey));
          }
        }
      }

      HolidayEventEntity entity = HolidayEventEntity.fromDTO(1.0, related, dto);
      entities.add(entity.withScore(strategy.score(entity) * coefficient));
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
      case HYPERBOLA:
        return new ScoreWrapper(new ScoreUtils.HyperbolaStrategy(start, end));
      case DIMENSION:
        return new DimensionStrategy();
      case COMPOUND:
        return new CompoundStrategy(new ScoreWrapper(new ScoreUtils.HyperbolaStrategy(start, end)));
      default:
        throw new IllegalArgumentException(String.format("Invalid strategy type '%s'", this.strategy));
    }
  }

  private interface ScoringStrategy {
    double score(HolidayEventEntity entity);
  }

  /**
   * Wrapper for ScoreUtils time-based strategies
   */
  private static class ScoreWrapper implements ScoringStrategy {
    private final ScoreUtils.TimeRangeStrategy delegate;

    ScoreWrapper(ScoreUtils.TimeRangeStrategy delegate) {
      this.delegate = delegate;
    }

    @Override
    public double score(HolidayEventEntity entity) {
      return this.delegate.score(entity.getDto().getStartTime(), entity.getDto().getEndTime());
    }
  }

  /**
   * Uses the highest score of dimension entities as they relate to an event
   */
  private static class DimensionStrategy implements ScoringStrategy {
    @Override
    public double score(HolidayEventEntity entity) {
      double max = 0.0;
      for(Entity r : entity.getRelated()) {
        if(r instanceof DimensionEntity) {
          final DimensionEntity de = (DimensionEntity) r;
          if (de.getName().equals(DIMENSION_COUNTRY_CODE)) {
            max = Math.max(de.getScore(), max);
          }
        }
      }
      return max;
    }
  }

  /**
   * Compound strategy that considers both event time as well as the presence of related dimension entities
   */
  private static class CompoundStrategy implements ScoringStrategy {
    private final ScoringStrategy delegateTime;
    private final ScoringStrategy delegateDimension = new DimensionStrategy();

    CompoundStrategy(ScoringStrategy delegateTime) {
      this.delegateTime = delegateTime;
    }

    @Override
    public double score(HolidayEventEntity entity) {
      double scoreTime = this.delegateTime.score(entity);
      double scoreDimension = this.delegateDimension.score(entity);
      double scoreHasDimension = scoreDimension > 0 ? 1 : 0;

      // ignore truncated results
      if (scoreTime <= 0)
        return 0;

      return scoreTime + scoreHasDimension + Math.min(scoreDimension, 1);
    }
  }
}
