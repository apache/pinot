/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.util.Predicate;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.MaxScoreSet;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import com.linkedin.thirdeye.rootcause.util.EntityUtils;
import com.linkedin.thirdeye.rootcause.util.ScoreUtils;
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
 * ThirdEyeEventsPipeline produces EventEntities within the current
 * TimeRange. It matches holidays and customized events based on incoming DimensionEntities (e.g. from contribution
 * analysis) and scores them based on the number of matching DimensionEntities.
 * This pipeline will add a buffer of 2 days to the time range provided
 */
public class ThirdEyeEventsPipeline extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(ThirdEyeEventsPipeline.class);

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
  private static final String PROP_EVENT_TYPE = "eventType";

  private static final long OVERFETCH = TimeUnit.DAYS.toMillis(2);

  private final StrategyType strategy;
  private final EventManager eventDAO;
  private final int k;
  private final String eventType;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param eventDAO event DAO
   * @param strategy scoring strategy
   * @param k the k
   * @param eventType the event type
   */
  public ThirdEyeEventsPipeline(String outputName, Set<String> inputNames, EventManager eventDAO, StrategyType strategy, int k, String eventType) {
    super(outputName, inputNames);
    this.eventDAO = eventDAO;
    this.strategy = strategy;
    this.k = k;
    this.eventType = eventType;
  }

  /**
   * Alternate constructor for RCAFrameworkLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param properties configuration properties ({@code PROP_STRATEGY})
   */
  public ThirdEyeEventsPipeline(String outputName, Set<String> inputNames, Map<String, Object> properties) {
    super(outputName, inputNames);
    this.eventDAO = DAORegistry.getInstance().getEventDAO();
    this.strategy = StrategyType.valueOf(MapUtils.getString(properties, PROP_STRATEGY, PROP_STRATEGY_DEFAULT));
    this.eventType = MapUtils.getString(properties, PROP_EVENT_TYPE, "holiday");
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

    Set<EventEntity> entities = new MaxScoreSet<>();
    entities.addAll(EntityUtils.addRelated(score(strategyAnomaly,
        this.getThirdEyeEvents(analysis.getStart(), anomaly.getEnd()), countryCodeLookup, anomaly.getScore()), anomaly));
    entities.addAll(EntityUtils.addRelated(score(strategyBaseline,
        this.getThirdEyeEvents(baseline.getStart(), baseline.getEnd()), countryCodeLookup, baseline.getScore()), baseline));

    return new PipelineResult(context, EntityUtils.topk(entities, this.k));
  }

  private List<EventDTO> getThirdEyeEvents(long start, long end) {
    return this.eventDAO.findByPredicate(Predicate.AND(
        Predicate.GE("startTime", start - OVERFETCH),
        Predicate.LT("endTime", end + OVERFETCH),
        Predicate.EQ("eventType", eventType.toUpperCase())
    ));
  }

  /* **************************************************************************
   * Entity scoring
   * *************************************************************************/
  private List<EventEntity> score(ScoringStrategy strategy, Iterable<EventDTO> events, Map<String, DimensionEntity> countryCodeLookup, double coefficient) {
    List<EventEntity> entities = new ArrayList<>();
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

      ThirdEyeEventEntity entity = ThirdEyeEventEntity.fromDTO(1.0, related, dto, eventType);
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
    double score(ThirdEyeEventEntity entity);
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
    public double score(ThirdEyeEventEntity entity) {
      return this.delegate.score(entity.getDto().getStartTime(), entity.getDto().getEndTime());
    }
  }

  /**
   * Uses the highest score of dimension entities as they relate to an event
   */
  private static class DimensionStrategy implements ScoringStrategy {
    @Override
    public double score(ThirdEyeEventEntity entity) {
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
    public double score(ThirdEyeEventEntity entity) {
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
