package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.anomaly.events.EventDataProviderManager;
import com.linkedin.thirdeye.anomaly.events.EventFilter;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.ExecutionContext;
import com.linkedin.thirdeye.rootcause.Metadata;
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
  EventDataProviderManager manager;

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public PipelineResult run(ExecutionContext context) {
    List<Entity> metrics = new ArrayList<>();
    for(Entity e : context.getSearchContext().getEntities()) {
      if(EntityUtils.isMetricEntity(e))
        metrics.add(e);
    }

    Set<EventDTO> eventSet = new HashSet<>();
    for(Entity e : metrics) {
      EventFilter filter = new EventFilter();
      filter.setMetricName(EntityUtils.getMetricEntityName(e));

      eventSet.addAll(manager.getEvents(filter));
    }

    List<EventDTO> events = new ArrayList<>(eventSet);
    Collections.sort(events, new Comparator<EventDTO>() {
      @Override
      public int compare(EventDTO o1, EventDTO o2) {
        return Long.compare(o1.getStartTime(), o2.getStartTime());
      }
    });

    Map<Entity, Double> scores = new HashMap<>();
    Map<Entity, Metadata> metadata = new HashMap<>();

    // Event URN: thirdeye:event:type:name:start
    for(EventDTO dto : events) {
      Entity e = EventEntityUtils.entityFromDTO(dto);

      scores.put(e, 1.0d);

      EventMetadata meta = new EventMetadata(dto);
      metadata.put(e, meta);
    }

    return new PipelineResult(scores, metadata);
  }
}
