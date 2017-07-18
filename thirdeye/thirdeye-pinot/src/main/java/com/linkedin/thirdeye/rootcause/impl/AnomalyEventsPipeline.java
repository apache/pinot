package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
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

  private static final String PROP_K = "k";
  private static final int PROP_K_DEFAULT = -1;

  private static final String PROP_LOOKBACK = "lookback";
  private static final String PROP_LOOKBACK_DEFAULT = "2d";

  private static final String PROP_STRATEGY = "strategy";
  private static final String PROP_STRATEGY_DEFAULT = ScoreUtils.StrategyType.QUADRATIC.toString();

  private final ScoreUtils.StrategyType strategy;
  private final MergedAnomalyResultManager anomalyDAO;
  private final long lookback;
  private final int k;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param anomalyDAO anomaly config DAO
   * @param lookback lookback period in millis
   */
  public AnomalyEventsPipeline(String outputName, Set<String> inputNames, MergedAnomalyResultManager anomalyDAO, ScoreUtils.StrategyType strategy, long lookback, int k) {
    super(outputName, inputNames);
    this.anomalyDAO = anomalyDAO;
    this.strategy = strategy;
    this.lookback = lookback;
    this.k = k;
  }

  /**
   * Alternate constructor for use by RCAFrameworkLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param properties configuration properties ({@code PROP_K}, {@code PROP_LOOKBACK}, {@code PROP_STRATEGY})
   */
  public AnomalyEventsPipeline(String outputName, Set<String> inputNames, Map<String, Object> properties) {
    super(outputName, inputNames);
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
    this.lookback = ScoreUtils.parsePeriod(MapUtils.getString(properties, PROP_LOOKBACK, PROP_LOOKBACK_DEFAULT));
    this.strategy = ScoreUtils.parseStrategy(MapUtils.getString(properties, PROP_STRATEGY, PROP_STRATEGY_DEFAULT));
    this.k = MapUtils.getInteger(properties, PROP_K, PROP_K_DEFAULT);
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<MetricEntity> metrics = context.filter(MetricEntity.class);

    TimeRangeEntity current = TimeRangeEntity.getContextCurrent(context);
    long lookback = current.getStart() - this.lookback;
    long start = current.getStart();
    long end = current.getEnd();

    ScoreUtils.TimeRangeStrategy strategy = ScoreUtils.build(this.strategy, lookback, start, end);

    Set<AnomalyEventEntity> entities = new HashSet<>();
    for(MetricEntity me : metrics) {
      List<MergedAnomalyResultDTO> anomalies = this.anomalyDAO.findAnomaliesByMetricIdAndTimeRange(me.getId(), lookback, end);

      for(MergedAnomalyResultDTO dto : anomalies) {
        double score = strategy.score(dto.getStartTime(), dto.getEndTime());
        entities.add(AnomalyEventEntity.fromDTO(score, dto));
      }
    }

    return new PipelineResult(context, EntityUtils.topk(entities, this.k));
  }
}
