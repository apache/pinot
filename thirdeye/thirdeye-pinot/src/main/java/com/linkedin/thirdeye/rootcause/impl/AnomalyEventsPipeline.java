package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.anomaly.events.EventDataProviderManager;
import com.linkedin.thirdeye.anomaly.events.EventFilter;
import com.linkedin.thirdeye.anomaly.events.EventType;
import com.linkedin.thirdeye.datalayer.bao.MetricConfigManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.dto.MetricConfigDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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

  private final EventDataProviderManager manager;
  private final MetricConfigManager metricDAO;

  /**
   * Constructor for dependency injection
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param manager event data provider manager
   * @param metricDAO metric config DAO
   */
  public AnomalyEventsPipeline(String outputName, Set<String> inputNames, EventDataProviderManager manager, MetricConfigManager metricDAO) {
    super(outputName, inputNames);
    this.manager = manager;
    this.metricDAO = metricDAO;
  }

  /**
   * Alternate constructor for use by PipelineLoader
   *
   * @param outputName pipeline output name
   * @param inputNames input pipeline names
   * @param ignore configuration properties (none)
   */
  public AnomalyEventsPipeline(String outputName, Set<String> inputNames, Map<String, String> ignore) {
    super(outputName, inputNames);
    this.manager = EventDataProviderManager.getInstance();
    this.metricDAO = DAORegistry.getInstance().getMetricConfigDAO();
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<MetricEntity> metrics = context.filter(MetricEntity.class);

    TimeRangeEntity current = TimeRangeEntity.getContextCurrent(context);

    Set<EventEntity> entities = new HashSet<>();
    for(MetricEntity me : metrics) {
      MetricConfigDTO metricDTO = this.metricDAO.findById(me.getId());
      if(metricDTO == null) {
        LOG.warn("Could not resolve metric id {}. Skipping.", me.getId());
        continue;
      }

      EventFilter filter = new EventFilter();
      filter.setEventType(EventType.HISTORICAL_ANOMALY.toString());
      filter.setMetricName(metricDTO.getName());

      for(EventDTO eventDTO : manager.getEvents(filter)) {
        double score = getScore(current, eventDTO);
        entities.add(EventEntity.fromDTO(score, eventDTO));
      }
    }

    return new PipelineResult(context, entities);
  }

  /**
   * Compute event score based on distance to the end of the current time window. Closer is better.
   *
   * @param current current time range
   * @param dto event dto
   * @return event entity score
   */
  private double getScore(TimeRangeEntity current, EventDTO dto) {
    long duration = current.getEnd() - current.getStart();
    long distance = dto.getEndTime() - current.getEnd();
    return 1.0 - distance / (double)duration;
  }
}
