package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.rootcause.MaxScoreSet;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Pipeline for identifying anomaly events based on their associated metric
 * names. The pipeline identifies metric entities in the search context and then invokes the
 * event provider manager to fetch any matching events. It then scores events based on their
 * time distance from the end of the search time window (closer is better).
 */
public class AnomalyEventsPipeline extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyEventsPipeline.class);

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

  private final StrategyType strategy;
  private final MergedAnomalyResultManager anomalyDAO;
  private final int k;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param anomalyDAO anomaly config DAO
   * @param strategy scoring strategy
   */
  public AnomalyEventsPipeline(String outputName, Set<String> inputNames, MergedAnomalyResultManager anomalyDAO, StrategyType strategy, int k) {
    super(outputName, inputNames);
    this.anomalyDAO = anomalyDAO;
    this.strategy = strategy;
    this.k = k;
  }

  /**
   * Alternate constructor for use by RCAFrameworkLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param properties configuration properties ({@code PROP_K}, {@code PROP_STRATEGY})
   */
  public AnomalyEventsPipeline(String outputName, Set<String> inputNames, Map<String, Object> properties) {
    super(outputName, inputNames);
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.strategy = StrategyType.valueOf(MapUtils.getString(properties, PROP_STRATEGY, PROP_STRATEGY_DEFAULT));
    this.k = MapUtils.getInteger(properties, PROP_K, PROP_K_DEFAULT);
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<MetricEntity> metrics = context.filter(MetricEntity.class);

    TimeRangeEntity anomaly = TimeRangeEntity.getTimeRangeAnomaly(context);
    TimeRangeEntity baseline = TimeRangeEntity.getTimeRangeBaseline(context);
    TimeRangeEntity analysis = TimeRangeEntity.getTimeRangeAnalysis(context);

    // use both provided and generated
    Set<DimensionEntity> dimensionEntities = context.filter(DimensionEntity.class);
    Map<String, DimensionEntity> urn2entity = EntityUtils.mapEntityURNs(dimensionEntities);

    ScoringStrategy strategyAnomaly = makeStrategy(analysis.getStart(), anomaly.getStart(), anomaly.getEnd());
    ScoringStrategy strategyBaseline = makeStrategy(baseline.getStart(), baseline.getStart(), baseline.getEnd());

    Set<AnomalyEventEntity> entities = new MaxScoreSet<>();
    for(MetricEntity me : metrics) {
      entities.addAll(EntityUtils.addRelated(score(strategyAnomaly,
          this.anomalyDAO.findAnomaliesByMetricIdAndTimeRange(me.getId(), analysis.getStart(), anomaly.getEnd()),
          urn2entity, anomaly.getScore() * me.getScore()), Arrays.asList(anomaly, me)));
      entities.addAll(EntityUtils.addRelated(score(strategyBaseline,
          this.anomalyDAO.findAnomaliesByMetricIdAndTimeRange(me.getId(), baseline.getStart(), baseline.getEnd()),
          urn2entity, baseline.getScore() * me.getScore()), Arrays.asList(baseline, me)));
    }

    return new PipelineResult(context, EntityUtils.topk(entities, this.k));
  }

  private List<AnomalyEventEntity> score(ScoringStrategy strategy, Iterable<MergedAnomalyResultDTO> anomalies, Map<String, DimensionEntity> urn2entity, double coefficient) {
    List<AnomalyEventEntity> entities = new ArrayList<>();
    for(MergedAnomalyResultDTO dto : anomalies) {
      double score = strategy.score(dto, urn2entity) * coefficient;
      entities.add(AnomalyEventEntity.fromDTO(score, dto));
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
        return new CompoundStrategy(new ScoreUtils.HyperbolaStrategy(start, end));
      default:
        throw new IllegalArgumentException(String.format("Invalid strategy type '%s'", this.strategy));
    }
  }

  private interface ScoringStrategy {
    double score(MergedAnomalyResultDTO dto, Map<String, DimensionEntity> urn2entity);
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
    public double score(MergedAnomalyResultDTO dto, Map<String, DimensionEntity> urn2entity) {
      return this.delegate.score(dto.getStartTime(), dto.getEndTime());
    }
  }

  /**
   * Uses the highest score of dimension entities as they relate to an event
   */
  private static class DimensionStrategy implements ScoringStrategy {
    @Override
    public double score(MergedAnomalyResultDTO dto, Map<String, DimensionEntity> urn2entity) {
      return makeDimensionScore(urn2entity, dto.getDimensions());
    }

    private static double makeDimensionScore(Map<String, DimensionEntity> urn2entity, Map<String, String> dimensions) {
      double max = 0.0;
      for(DimensionEntity e : filter2entities(dimensions)) {
        if(urn2entity.containsKey(e.getUrn())) {
          max = Math.max(urn2entity.get(e.getUrn()).getScore(), max);
        }
      }
      return max;
    }

    private static Set<DimensionEntity> filter2entities(Map<String, String> dimensions) {
      Set<DimensionEntity> entities = new HashSet<>();
      for(Map.Entry<String, String> e : dimensions.entrySet()) {
        String name = e.getKey();
        String val = e.getValue();
        entities.add(DimensionEntity.fromDimension(1.0, name, val.toLowerCase(), DimensionEntity.TYPE_GENERATED));
      }
      return entities;
    }
  }

  /**
   * Compound strategy that considers both event time as well as the presence of related dimension entities
   */
  private static class CompoundStrategy implements ScoringStrategy {
    private final ScoreUtils.TimeRangeStrategy delegateTime;
    private final ScoringStrategy delegateDimension = new DimensionStrategy();

    CompoundStrategy(ScoreUtils.TimeRangeStrategy delegateTime) {
      this.delegateTime = delegateTime;
    }

    @Override
    public double score(MergedAnomalyResultDTO dto, Map<String, DimensionEntity> urn2entity) {
      double scoreTime = this.delegateTime.score(dto.getStartTime(), dto.getEndTime());
      double scoreDimension = this.delegateDimension.score(dto, urn2entity);
      double scoreHasDimension = scoreDimension > 0 ? 1 : 0;

      // ignore truncated results
      if (scoreTime <= 0)
        return 0;

      return scoreTime + scoreHasDimension + Math.min(scoreDimension, 1);
    }
  }
}
