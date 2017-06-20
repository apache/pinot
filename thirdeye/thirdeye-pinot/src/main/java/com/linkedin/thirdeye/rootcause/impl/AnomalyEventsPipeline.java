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
import org.joda.time.DateTime;
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

  private static final int START_OFFSET_HOURS = 6;

  private final MergedAnomalyResultManager anomalyDAO;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param anomalyDAO anomaly config DAO
   */
  public AnomalyEventsPipeline(String outputName, Set<String> inputNames, MergedAnomalyResultManager anomalyDAO) {
    super(outputName, inputNames);
    this.anomalyDAO = anomalyDAO;
  }

  /**
   * Alternate constructor for use by RCAFrameworkLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param ignore configuration properties (none)
   */
  public AnomalyEventsPipeline(String outputName, Set<String> inputNames, Map<String, Object> ignore) {
    super(outputName, inputNames);
    this.anomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<MetricEntity> metrics = context.filter(MetricEntity.class);

    TimeRangeEntity current = TimeRangeEntity.getContextCurrent(context);
    long start = new DateTime(current.getStart()).minusHours(START_OFFSET_HOURS).getMillis();
    long end = current.getEnd();

    Set<AnomalyEventEntity> entities = new HashSet<>();
    for(MetricEntity me : metrics) {
      List<MergedAnomalyResultDTO> anomalies = this.anomalyDAO.findAnomaliesByMetricIdAndTimeRange(me.getId(), start, end);

      for(MergedAnomalyResultDTO dto : anomalies) {
        double score = getScore(dto, start, end);
        entities.add(AnomalyEventEntity.fromDTO(score, dto));
      }
    }

    return new PipelineResult(context, EntityUtils.normalizeScores(entities));
  }

  private double getScore(MergedAnomalyResultDTO dto, long start, long end) {
    long duration = end - start;
    long offset = dto.getEndTime() - start;
    return Math.max(offset / (double)duration, 0);
  }
}
