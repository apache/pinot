package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.anomaly.events.EventDataProviderManager;
import com.linkedin.thirdeye.anomaly.events.EventFilter;
import com.linkedin.thirdeye.anomaly.events.EventType;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.ExecutionContext;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Sample implementation of a pipeline for identifying events based on their associated metric
 * names. The pipeline identifies metric entities in the search context and then invokes the
 * event provider manager to fetch any matching events. It then scores events based on their
 * time distance from the end of the search time window (closer is better).
 */
public class EventMetricPipeline implements Pipeline {
  private static final Logger LOG = LoggerFactory.getLogger(EventMetricPipeline.class);

  final EventDataProviderManager manager;

  public EventMetricPipeline(EventDataProviderManager manager) {
    this.manager = manager;
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public PipelineResult run(ExecutionContext context) {
    Set<Entity> metrics = EntityUtils.filterContext(context, EntityType.METRIC);

    TimeRangeEntity current = EntityUtils.getContextTimeRange(context);
    if(current == null) {
      LOG.warn("Pipeline '{}' requires TimeRangeEntity. Skipping.", this.getName());
      return new PipelineResult(Collections.<Entity>emptyList());
    }

    Set<EventDTO> events = new HashSet<>();
    for(Entity e : metrics) {
      EventFilter filter = new EventFilter();
      filter.setEventType(EventType.HISTORICAL_ANOMALY);
      filter.setMetricName(EntityUtils.getMetricName(e.getUrn()));

      events.addAll(manager.getEvents(filter));
    }

    List<EventEntity> entities = new ArrayList<>();
    for(EventDTO e : events) {
      long distance = current.getEnd() - e.getStartTime();
      double score = -distance;
      entities.add(EventEntity.fromDTO(score, e));
    }

    return new PipelineResult(entities);
  }
}
