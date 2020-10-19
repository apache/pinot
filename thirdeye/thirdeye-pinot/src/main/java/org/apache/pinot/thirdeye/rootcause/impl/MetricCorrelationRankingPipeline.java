/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.thirdeye.rootcause.impl;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.pinot.thirdeye.dataframe.DataFrame;
import org.apache.pinot.thirdeye.dataframe.util.DataFrameUtils;
import org.apache.pinot.thirdeye.dataframe.DoubleSeries;
import org.apache.pinot.thirdeye.dataframe.Series;
import org.apache.pinot.thirdeye.dataframe.util.MetricSlice;
import org.apache.pinot.thirdeye.dataframe.util.TimeSeriesRequestContainer;
import org.apache.pinot.thirdeye.datalayer.bao.DatasetConfigManager;
import org.apache.pinot.thirdeye.datalayer.bao.MetricConfigManager;
import org.apache.pinot.thirdeye.datasource.DAORegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeCacheRegistry;
import org.apache.pinot.thirdeye.datasource.ThirdEyeRequest;
import org.apache.pinot.thirdeye.datasource.ThirdEyeResponse;
import org.apache.pinot.thirdeye.datasource.cache.QueryCache;
import org.apache.pinot.thirdeye.rootcause.Entity;
import org.apache.pinot.thirdeye.rootcause.Pipeline;
import org.apache.pinot.thirdeye.rootcause.PipelineContext;
import org.apache.pinot.thirdeye.rootcause.PipelineResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The MetricCorrelationRankingPipeline ranks metrics based on their similarity to a set of
 * target metrics. It takes two sets of MetricEntities as inputs - candidate and target
 * metrics - and ranks the candidate metrics based on the absolute strength of their
 * correlation with the target metrics over the given time range.
 *
 * @see MetricAnalysisPipeline as a replacement that relies on MetricEntities with filters
 * in the tail of the URN
 */
@Deprecated
public class MetricCorrelationRankingPipeline extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(MetricCorrelationRankingPipeline.class);

  private static final long TIMEOUT = 60000;

  private static final String PROP_TARGET_INPUT = "targetInput";
  private static final String PROP_STRATEGY = "strategy";

  private static final String PRE_CURRENT = "current:";
  private static final String PRE_BASELINE = "baseline:";

  private static final String COL_TIME = DataFrame.COL_TIME;
  private static final String COL_VALUE = DataFrame.COL_VALUE;
  private static final String COL_CURRENT = "current";
  private static final String COL_BASELINE = "baseline";
  private static final String COL_CHANGE = "change";
  private static final String COL_TARGET = "target";
  private static final String COL_CANDIDATE = "candidate";

  private static final String STRATEGY_CORRELATION = "correlation";
  private static final String STRATEGY_EUCLIDEAN = "euclidean";
  private static final String STRATEGY_STATIC = "static";
  private static final String STRATEGY_CANDIDATE_MEAN = "candidate_mean";
  private static final String STRATEGY_CANDIDATE_MAX = "candidate_max";

  private final QueryCache cache;
  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private final String targetInput;
  private final ScoringStrategy strategy;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param targetInput input pipeline name for target metrics
   * @param strategy scoring strategy for differences
   * @param cache query cache
   * @param metricDAO metric config DAO
   * @param datasetDAO datset config DAO
   */
  public MetricCorrelationRankingPipeline(String outputName, Set<String> inputNames, String targetInput, ScoringStrategy strategy, QueryCache cache, MetricConfigManager metricDAO, DatasetConfigManager datasetDAO) {
    super(outputName, inputNames);
    this.targetInput = targetInput;
    this.cache = cache;
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
    this.strategy = strategy;
  }

  /**
   * Alternate constructor for RCAFrameworkLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param properties configuration properties ({@code PROP_TARGET_INPUT})
   */
  public MetricCorrelationRankingPipeline(String outputName, Set<String> inputNames, Map<String, Object> properties) {
    super(outputName, inputNames);
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.cache = ThirdEyeCacheRegistry.getInstance().getQueryCache();

    if(!properties.containsKey(PROP_TARGET_INPUT))
      throw new IllegalArgumentException(String.format("Property '%s' required, but not found.", PROP_TARGET_INPUT));
    this.targetInput = properties.get(PROP_TARGET_INPUT).toString();

    String propStrategy = STRATEGY_CORRELATION;
    if(properties.containsKey(PROP_STRATEGY))
      propStrategy = properties.get(PROP_STRATEGY).toString();
    this.strategy = parseStrategy(propStrategy);
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<MetricEntity> candidateMetrics = filterNonBaselineContext(context);

    TimeRangeEntity anomalyRange = TimeRangeEntity.getTimeRangeAnomaly(context);
    TimeRangeEntity baselineRange = TimeRangeEntity.getTimeRangeBaseline(context);

    Multimap<String, String> filters = DimensionEntity.makeFilterSet(context);

    Set<MetricEntity> targetMetrics = new HashSet<>();
    for(Entity entity : context.getInputs().get(this.targetInput)) {
      if(entity instanceof MetricEntity) {
        targetMetrics.add((MetricEntity) entity);
      }
    }

    LOG.info("Processing {} target metrics and {} candidate metrics", targetMetrics.size(), candidateMetrics.size());

    Set<MetricEntity> allMetrics = new HashSet<>();
    allMetrics.addAll(candidateMetrics);
    allMetrics.addAll(targetMetrics);

    // generate requests
    List<TimeSeriesRequestContainer> requestList = new ArrayList<>();
    requestList.addAll(makeRequests(targetMetrics, anomalyRange, filters, PRE_CURRENT));
    requestList.addAll(makeRequests(candidateMetrics, anomalyRange, filters, PRE_CURRENT));
    requestList.addAll(makeRequests(targetMetrics, baselineRange, filters, PRE_BASELINE));
    requestList.addAll(makeRequests(candidateMetrics, baselineRange, filters, PRE_BASELINE));

    LOG.info("Requesting {} time series", requestList.size());

    Map<String, TimeSeriesRequestContainer> requests = new HashMap<>();
    for(TimeSeriesRequestContainer rc : requestList) {
      requests.put(rc.getRequest().getRequestReference(), rc);
    }

    // fetch responses and calculate derived metrics
    Map<String, DataFrame> responses = new HashMap<>();
    List<ThirdEyeRequest> thirdeyeRequests = new ArrayList<>();
    for(TimeSeriesRequestContainer rc : requestList) {
      thirdeyeRequests.add(rc.getRequest());
    }

      // send requests
    Collection<Future<ThirdEyeResponse>> futures = submitRequests(thirdeyeRequests);

    int i = 0;
    for(Future<ThirdEyeResponse> future : futures) {
      // fetch response
      ThirdEyeResponse response;
      try {
        response = future.get(TIMEOUT, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        LOG.warn("Error executing request '{}'. Skipping.", requestList.get(i).getRequest().getRequestReference(), e);
        continue;
      } finally {
        i++;
      }

      // parse time series
      String id = response.getRequest().getRequestReference();
      DataFrame df;
      try {
        df = DataFrameUtils.evaluateResponse(response, requests.get(id));
      } catch (Exception e) {
        LOG.warn("Could not parse response for '{}'. Skipping.", id, e);
        continue;
      }

      if(LOG.isDebugEnabled()) {
        LOG.debug("DataFrame '{}':\n{}", id, df);
      }

      // store time series
      responses.put(id, df);
    }

    // determine current-baseline changes
    final long timeRangeDiff = anomalyRange.getStart() - baselineRange.getStart();

    Map<MetricEntity, DataFrame> pctChanges = new HashMap<>();
    for(MetricEntity entity : allMetrics) {
      String currentId = makeIdentifier(entity.getUrn(), PRE_CURRENT);
      String baselineId = makeIdentifier(entity.getUrn(), PRE_BASELINE);

      if(!responses.containsKey(currentId)) {
        LOG.warn("No current data for '{}'. Skipping.", entity.getUrn());
        continue;
      }

      if(!responses.containsKey(baselineId)) {
        LOG.warn("No baseline data for '{}'. Skipping.", entity.getUrn());
        continue;
      }

      LOG.info("Preparing data for metric '{}'", entity.getUrn());
      DataFrame current = new DataFrame(responses.get(currentId)).renameSeries(COL_VALUE, COL_CURRENT);
      DataFrame baseline = new DataFrame(responses.get(baselineId)).renameSeries(COL_VALUE, COL_BASELINE);

      baseline.addSeries(COL_TIME, baseline.getLongs(COL_TIME).add(timeRangeDiff));

      DataFrame joined = current.joinInner(baseline);

      joined.mapInPlace(new Series.DoubleFunction() {
        @Override
        public double apply(double... values) {
          return values[0] / values[1] - 1;
        }
      }, COL_CHANGE, COL_CURRENT, COL_BASELINE);
      joined.dropSeries(COL_CURRENT);
      joined.dropSeries(COL_BASELINE);

      pctChanges.put(entity, joined);
    }

    // determine score
    Map<MetricEntity, Double> scores = new HashMap<>();
    Multimap<MetricEntity, MetricEntity> related = ArrayListMultimap.create();

    LOG.info("Using scoring strategy '{}'", this.strategy.getClass().getSimpleName());
    for(MetricEntity targetMetric : targetMetrics) {
      if(!pctChanges.containsKey(targetMetric)) {
        LOG.warn("No diff data for target metric '{}'. Skipping.", targetMetric.getUrn());
        continue;
      }

      DataFrame changesTarget = new DataFrame(pctChanges.get(targetMetric))
          .renameSeries(COL_CHANGE, COL_TARGET);

      for(MetricEntity candidateMetric : candidateMetrics) {
        if(!pctChanges.containsKey(candidateMetric)) {
          LOG.warn("No diff data for candidate metric '{}'. Skipping.", candidateMetric.getUrn());
          continue;
        }

        LOG.info("Calculating score for metric '{}'", candidateMetric.getUrn());
        DataFrame changesCandidate = new DataFrame(pctChanges.get(candidateMetric))
            .renameSeries(COL_CHANGE, COL_CANDIDATE);

        DataFrame joined = changesTarget.joinInner(changesCandidate);

        try {
          double score =
              this.strategy.score(joined.getDoubles(COL_TARGET), joined.getDoubles(COL_CANDIDATE))
              * targetMetric.getScore() * candidateMetric.getScore();

          LOG.debug("Score for target '{}' and candidate '{}' is {} (based on {} data points)", targetMetric.getUrn(), candidateMetric.getUrn(), score, joined.size());

          if (!scores.containsKey(candidateMetric)) {
            scores.put(candidateMetric, 0.0);
          }
          scores.put(candidateMetric, Math.max(scores.get(candidateMetric), score));

          related.put(candidateMetric, targetMetric);

        } catch (Exception e) {
          LOG.warn("Could not calculate correlation of target '{}' and candidate '{}'. Skipping.", targetMetric.getUrn(), candidateMetric.getUrn(), e);
        }
      }
    }

    // generate output
    Set<MetricEntity> entities = new HashSet<>();
    for(MetricEntity me : scores.keySet()) {
      entities.add(me.withScore(scores.get(me)).withRelated(new ArrayList<Entity>(related.get(me))));
    }

    LOG.info("Generated {} MetricEntities with valid scores", entities.size());

    return new PipelineResult(context, entities);
  }

  private Collection<Future<ThirdEyeResponse>> submitRequests(List<ThirdEyeRequest> thirdeyeRequests) {
    try {
      return this.cache.getQueryResultsAsync(thirdeyeRequests).values();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private List<TimeSeriesRequestContainer> makeRequests(Collection<MetricEntity> metrics, TimeRangeEntity range, Multimap<String, String> filters, String prefix) {
    List<TimeSeriesRequestContainer> requests = new ArrayList<>();
    for(MetricEntity e : metrics) {
      String id = makeIdentifier(e.getUrn(), prefix);
      MetricSlice slice = MetricSlice.from(e.getId(), range.getStart(), range.getEnd(), filters);
      try {
        requests.add(DataFrameUtils.makeTimeSeriesRequest(slice, id, this.metricDAO, this.datasetDAO));
      } catch (Exception ex) {
        LOG.warn(String.format("Could not make request for '%s'. Skipping.", id), ex);
      }
    }
    return requests;
  }

  private Set<MetricEntity> filterNonBaselineContext(PipelineContext context) {
    Set<MetricEntity> filtered = new HashSet<>();
    for(Map.Entry<String, Set<Entity>> input : context.getInputs().entrySet()) {
      if(this.targetInput.equals(input.getKey()))
        continue;

      for (Entity e : input.getValue()) {
        if(MetricEntity.class.isInstance(e)) {
          filtered.add((MetricEntity) e);
        }
      }
    }
    return filtered;
  }

  private static String makeIdentifier(String urn, String prefix) {
    return prefix + urn;
  }

  private static ScoringStrategy parseStrategy(String strategy) {
    switch(strategy) {
      case STRATEGY_CORRELATION:
        return new CorrelationStrategy();
      case STRATEGY_EUCLIDEAN:
        return new EuclideanStrategy();
      case STRATEGY_STATIC:
        return new StaticStrategy();
      case STRATEGY_CANDIDATE_MEAN:
        return new CandidateMeanChangeStrategy();
      case STRATEGY_CANDIDATE_MAX:
        return new CandidateMaxChangeStrategy();
      default:
        throw new IllegalArgumentException(String.format("Unknown strategy '%s'", strategy));
    }
  }

  private interface ScoringStrategy {
    double score(DoubleSeries target, DoubleSeries candidate);
  }

  private static class CorrelationStrategy implements ScoringStrategy {
    @Override
    public double score(DoubleSeries target, DoubleSeries candidate) {
      // NOTE: higher correlation is better (positive and negative)
      return Math.abs(candidate.corr(target));
    }
  }

  private static class EuclideanStrategy implements ScoringStrategy {
    @Override
    public double score(DoubleSeries target, DoubleSeries candidate) {
      // NOTE: closer is better
      return 1.0 / Math.sqrt(candidate.subtract(target).pow(2).sum().value());
    }
  }

  private static class StaticStrategy implements ScoringStrategy {
    @Override
    public double score(DoubleSeries target, DoubleSeries candidate) {
      return 1.0;
    }
  }

  private static class CandidateMeanChangeStrategy implements ScoringStrategy {
    @Override
    public double score(DoubleSeries target, DoubleSeries candidate) {
      return candidate.mean().abs().doubleValue();
    }
  }

  private static class CandidateMaxChangeStrategy implements ScoringStrategy {
    @Override
    public double score(DoubleSeries target, DoubleSeries candidate) {
      return candidate.abs().max().doubleValue();
    }
  }
}
