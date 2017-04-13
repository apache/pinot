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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class EventMetricPipeline implements Pipeline {
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
    Set<Entity> metrics = URNUtils.filterContext(context, URNUtils.EntityType.METRIC);

    Set<EventDTO> eventSet = new HashSet<>();
    for(Entity e : metrics) {
      EventFilter filter = new EventFilter();
      filter.setStartTime(0);
      if(context.getSearchContext().getTimestampEnd() >= 0)
        filter.setEndTime(context.getSearchContext().getTimestampEnd());
      filter.setEventType(EventType.HISTORICAL_ANOMALY);
      filter.setMetricName(URNUtils.getMetricName(e.getUrn()));

      eventSet.addAll(manager.getEvents(filter));
    }

    List<EventDTO> events = new ArrayList<>(eventSet);
    Collections.sort(events, new Comparator<EventDTO>() {
      @Override
      public int compare(EventDTO o1, EventDTO o2) {
        return Long.compare(o1.getStartTime(), o2.getStartTime());
      }
    });

    Map<EventEntity, Double> scores = new HashMap<>();

    int i = 0;
    for(EventDTO dto : events) {
      EventEntity e = EventEntity.fromDTO(dto);

      double score = i++ / (double)events.size();
      scores.put(e, score);
    }

    return new PipelineResult(scores);
  }
}
