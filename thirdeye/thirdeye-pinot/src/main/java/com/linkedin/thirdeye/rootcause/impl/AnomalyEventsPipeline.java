package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.anomaly.events.EventDataProviderManager;
import com.linkedin.thirdeye.anomaly.events.EventFilter;
import com.linkedin.thirdeye.anomaly.events.EventType;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineContext;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.HashSet;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Sample implementation of a pipeline for identifying anomaly events based on their associated metric
 * names. The pipeline identifies metric entities in the search context and then invokes the
 * event provider manager to fetch any matching events. It then scores events based on their
 * time distance from the end of the search time window (closer is better).
 */
public class AnomalyEventsPipeline extends Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(AnomalyEventsPipeline.class);

  final EventDataProviderManager manager;

  public AnomalyEventsPipeline(String name, Set<String> inputs, EventDataProviderManager manager) {
    super(name, inputs);
    this.manager = manager;
  }

  @Override
  public PipelineResult run(PipelineContext context) {
    Set<MetricEntity> metrics = EntityUtils.filterContext(context, MetricEntity.class);

    TimeRangeEntity current = TimeRangeEntity.getContextCurrent(context);
    long duration = current.getEnd() - current.getStart();

    Set<EventEntity> entities = new HashSet<>();
    for(MetricEntity e : metrics) {
      EventFilter filter = new EventFilter();
      filter.setEventType(EventType.HISTORICAL_ANOMALY.toString());
      filter.setMetricName(e.getMetric());

      for(EventDTO dto : manager.getEvents(filter)) {
        long distance = current.getEnd() - dto.getStartTime();
        double score = 1.0 - distance / (double)duration;
        entities.add(EventEntity.fromDTO(score, dto));
      }
    }

    return new PipelineResult(context, entities);
  }
}
