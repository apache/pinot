package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.client.MetricExpression;
import com.linkedin.thirdeye.client.MetricFunction;
import com.linkedin.thirdeye.client.ThirdEyeCacheRegistry;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.client.ThirdEyeResponse;
import com.linkedin.thirdeye.client.ThirdEyeResponseRow;
import com.linkedin.thirdeye.client.cache.QueryCache;
import com.linkedin.thirdeye.constant.MetricAggFunction;
import com.linkedin.thirdeye.dashboard.Utils;
import com.linkedin.thirdeye.dataframe.DataFrame;
import com.linkedin.thirdeye.dataframe.DoubleSeries;
import com.linkedin.thirdeye.dataframe.LongSeries;
import com.linkedin.thirdeye.dataframe.Series;
import com.linkedin.thirdeye.dataframe.StringSeries;
import com.linkedin.thirdeye.datalayer.bao.DatasetConfigManager;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MetricCorrelationRankingPipeline extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(MetricCorrelationRankingPipeline.class);

  private static final String PROP_TARGET_INPUT = "targetInput";

  private static final String PRE_CURRENT = "current:";
  private static final String PRE_BASELINE = "baseline:";

  private static final String COL_TIME = "timestamp";
  private static final String COL_VALUE = "value";

  private final QueryCache cache;
  private final MetricConfigManager metricDAO;
  private final DatasetConfigManager datasetDAO;
  private String targetInput;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param targetInput input pipeline name for target metrics
   * @param cache query cache
   * @param metricDAO metric config DAO
   * @param datasetDAO datset config DAO
   */
  public MetricCorrelationRankingPipeline(String outputName, Set<String> inputNames, String targetInput, QueryCache cache, MetricConfigManager metricDAO, DatasetConfigManager datasetDAO) {
    super(outputName, inputNames);
    this.targetInput = targetInput;
    this.cache = cache;
    this.metricDAO = metricDAO;
    this.datasetDAO = datasetDAO;
  }

  /**
   * Alternate constructor for PipelineLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param properties configuration properties ({@code PROP_TARGET_INPUT})
   */
  public MetricCorrelationRankingPipeline(String outputName, Set<String> inputNames, Map<String, String> properties) {
    super(outputName, inputNames);
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
    this.datasetDAO = DAORegistry.getInstance().getDatasetConfigDAO();
    this.cache = ThirdEyeCacheRegistry.getInstance().getQueryCache();

    if(!properties.containsKey(PROP_TARGET_INPUT))
      throw new IllegalArgumentException(String.format("Property '%s' required, but not found.", PROP_TARGET_INPUT));
    this.targetInput = properties.get(PROP_TARGET_INPUT);
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<MetricEntity> candidateMetrics = filterNonBaselineContext(context);

    TimeRangeEntity currentRange = TimeRangeEntity.getContextCurrent(context);
    TimeRangeEntity baselineRange = TimeRangeEntity.getContextBaseline(context);

    Set<MetricEntity> targetMetrics = new HashSet<>();
    for(Entity e : context.getInputs().get(this.targetInput)) {
      if(MetricEntity.class.isInstance(e))
        targetMetrics.add((MetricEntity) e);
    }

    LOG.info("Processing {} target metrics and {} candidate metrics", targetMetrics.size(), candidateMetrics.size());

    Set<MetricEntity> allMetrics = new HashSet<>();
    allMetrics.addAll(candidateMetrics);
    allMetrics.addAll(targetMetrics);

    // generate requests
    List<RequestContainer> requestList = new ArrayList<>();
    requestList.addAll(makeRequests(targetMetrics, currentRange, PRE_CURRENT));
    requestList.addAll(makeRequests(candidateMetrics, currentRange, PRE_CURRENT));
    requestList.addAll(makeRequests(targetMetrics, baselineRange, PRE_BASELINE));
    requestList.addAll(makeRequests(candidateMetrics, baselineRange, PRE_BASELINE));

    LOG.info("Requesting {} time series", requestList.size());

    Map<String, RequestContainer> requests = new HashMap<>();
    for(RequestContainer c : requestList)
      requests.put(c.request.getRequestReference(), c);

    // fetch responses and calculate derived metrics
    Map<String, DataFrame> responses = new HashMap<>();
    try {
      List<ThirdEyeRequest> thirdeyeRequests = new ArrayList<>();
      for(RequestContainer c : requestList)
        thirdeyeRequests.add(c.request);

      Collection<ThirdEyeResponse> result = this.cache.getQueryResultsAsyncAndWait(thirdeyeRequests).values();

      for(ThirdEyeResponse r : result) {
        String id = r.getRequest().getRequestReference();
        DataFrame df = parseResponse(r);
        evaluateExpressions(df, requests.get(id).expressions);
        if(LOG.isDebugEnabled())
          LOG.debug("DataFrame '{}':\n{}", id, df);

        responses.put(id, df);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // determine current-baseline changes
    Map<MetricEntity, DoubleSeries> pctChanges = new HashMap<>();
    for(MetricEntity e : allMetrics) {
      String currentId = makeIdentifier(e.getUrn(), PRE_CURRENT);
      String baselineId = makeIdentifier(e.getUrn(), PRE_BASELINE);

      if(!responses.containsKey(currentId)) {
        LOG.warn("No current data for '{}'. Skipping.", e.getUrn());
        continue;
      }

      if(!responses.containsKey(baselineId)) {
        LOG.warn("No baseline data for '{}'. Skipping.", e.getUrn());
        continue;
      }

      DataFrame current = responses.get(currentId);
      DataFrame baseline = responses.get(baselineId);

      DoubleSeries pctChange = current.joinInner(baseline).map(new Series.DoubleFunction() {
        @Override
        public double apply(double... values) {
          return (values[0] - values[1]) / values[1];
        }
      }, COL_VALUE, COL_VALUE + "_right");

      pctChanges.put(e, pctChange);
    }

    // determine relevance - by strength of correlation
    Map<MetricEntity, Double> scores = new HashMap<>();
    for(MetricEntity target : targetMetrics) {
      if(!pctChanges.containsKey(target)) {
        LOG.warn("No diff data for target metric '{}'. Skipping.", target.getUrn());
        continue;
      }

      DoubleSeries changesTarget = pctChanges.get(target);

      for(MetricEntity candidate : candidateMetrics) {
        if(!pctChanges.containsKey(target)) {
          LOG.warn("No diff data for candidate metric '{}'. Skipping.", candidate.getUrn());
          continue;
        }

        DoubleSeries changesCandidate = pctChanges.get(candidate);

        try {
          double score = Math.abs(changesTarget.corr(changesCandidate)) * target.getScore();

          if (!scores.containsKey(candidate))
            scores.put(candidate, 0.0);
          scores.put(candidate, scores.get(candidate) + score);

        } catch (Exception e) {
          LOG.warn("Could not calculate correlation of target '{}' and candidate '{}'. Skipping.", target.getUrn(), candidate.getUrn(), e);
        }
      }
    }

    // generate output
    Set<MetricEntity> entities = new HashSet<>();
    for(Map.Entry<MetricEntity, Double> entry : scores.entrySet()) {
      entities.add(entry.getKey().withScore(entry.getValue()));
    }

    LOG.info("Generated {} MetricEntities with valid scores", entities.size());

    return new PipelineResult(context, entities);
  }

  private List<RequestContainer> makeRequests(Collection<MetricEntity> metrics, TimeRangeEntity timerange, String prefix) {
    List<RequestContainer> requests = new ArrayList<>();
    for(MetricEntity e : metrics) {
      String id = makeIdentifier(e.getUrn(), prefix);
      try {
        requests.add(makeRequest(e, timerange, id));
      } catch (Exception ex) {
        LOG.warn(String.format("Could not make request for '%s'. Skipping.", id), ex);
      }
    }
    return requests;
  }

  private RequestContainer makeRequest(MetricEntity e, TimeRangeEntity t, String reference) throws Exception {
    MetricConfigDTO metric = this.metricDAO.findById(e.getId());
    if(metric == null)
      throw new IllegalArgumentException(String.format("Could not resolve metric id %d", e.getId()));

    DatasetConfigDTO dataset = this.datasetDAO.findByDataset(metric.getDataset());
    if(dataset == null)
      throw new IllegalArgumentException(String.format("Could not resolve dataset '%s' for metric id '%d'", metric.getDataset(), metric.getId()));

    List<MetricFunction> functions = new ArrayList<>();
    List<MetricExpression> expressions = Utils.convertToMetricExpressions(metric.getName(), MetricAggFunction.SUM, metric.getDataset());
    for(MetricExpression exp : expressions)
      functions.addAll(exp.computeMetricFunctions());

    ThirdEyeRequest request = ThirdEyeRequest.newBuilder()
        .setStartTimeInclusive(t.getStart())
        .setEndTimeExclusive(t.getEnd())
        .setMetricFunctions(functions)
        .setGroupBy(dataset.getTimeColumn())
        .setGroupByTimeGranularity(dataset.bucketTimeGranularity())
        .build(reference);

    return new RequestContainer(request, expressions);
  }

  private Set<MetricEntity> filterNonBaselineContext(PipelineContext context) {
    Set<MetricEntity> filtered = new HashSet<>();
    for(Map.Entry<String, Set<Entity>> input : context.getInputs().entrySet()) {
      if(this.targetInput.equals(input.getKey()))
        continue;
      for (Entity e : input.getValue()) {
        if(MetricEntity.class.isInstance(e))
          filtered.add((MetricEntity) e);
      }
    }
    return filtered;
  }

  private static String makeIdentifier(String urn, String prefix) {
    return prefix + urn;
  }

  private static DataFrame parseResponse(ThirdEyeResponse response) {
    // builders
    LongSeries.Builder timeBuilder = LongSeries.builder();
    List<StringSeries.Builder> dimensionBuilders = new ArrayList<>();
    List<DoubleSeries.Builder> functionBuilders = new ArrayList<>();

    for(int i=0; i<response.getGroupKeyColumns().size(); i++)
      dimensionBuilders.add(StringSeries.builder());

    for(int i=0; i<response.getMetricFunctions().size(); i++)
      functionBuilders.add(DoubleSeries.builder());

    // values
    for(int i=0; i<response.getNumRows(); i++) {
      ThirdEyeResponseRow r = response.getRow(i);
      timeBuilder.addValues(r.getTimeBucketId());

      for(int j=0; j<r.getDimensions().size(); j++) {
        dimensionBuilders.get(j).addValues(r.getDimensions().get(j));
      }

      for(int j=0; j<r.getMetrics().size(); j++) {
        functionBuilders.get(j).addValues(r.getMetrics().get(j));
      }
    }

    // dataframe
    String timeColumn = response.getDataTimeSpec().getColumnName();

    DataFrame df = new DataFrame();
    df.addSeries(COL_TIME, timeBuilder.build());
    df.setIndex(COL_TIME);

    int i = 0;
    for(String n : response.getGroupKeyColumns()) {
      if(!timeColumn.equals(n))
        df.addSeries(n, dimensionBuilders.get(i++).build());
    }

    int j = 0;
    for(MetricFunction mf : response.getMetricFunctions()) {
      df.addSeries(mf.toString(), functionBuilders.get(j++).build());
    }

    return df.sortedBy(COL_TIME);
  }

  private static DataFrame evaluateExpressions(DataFrame df, Collection<MetricExpression> expressions) throws Exception {
    if(expressions.size() != 1)
      throw new IllegalArgumentException("Requires exactly one expression");

    MetricExpression me = expressions.iterator().next();
    Collection<MetricFunction> functions = me.computeMetricFunctions();

    Map<String, Double> context = new HashMap<>();
    double[] values = new double[df.size()];

    for(int i=0; i<df.size(); i++) {
      for(MetricFunction f : functions) {
        // TODO check inconsistency between getMetricName() and toString()
        context.put(f.getMetricName(), df.getDouble(f.toString(), i));
      }
      values[i] = MetricExpression.evaluateExpression(me, context);
    }

    df.addSeries(COL_VALUE, values);

    return df;
  }

  private static class RequestContainer {
    final ThirdEyeRequest request;
    final List<MetricExpression> expressions;

    RequestContainer(ThirdEyeRequest request, List<MetricExpression> expressions) {
      this.request = request;
      this.expressions = expressions;
    }
  }
}
