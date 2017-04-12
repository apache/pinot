package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.anomaly.events.EventDataProviderManager;
import com.linkedin.thirdeye.anomaly.events.EventFilter;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.ExecutionContext;
import com.linkedin.thirdeye.rootcause.Metadata;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class EventTimePipeline implements Pipeline {
  EventDataProviderManager manager;

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public PipelineResult run(ExecutionContext context) {
    EventFilter filter = new EventFilter();

    if(context.getSearchContext().getTimestampStart() >= 0)
      filter.setStartTime(context.getSearchContext().getTimestampStart());

    if(context.getSearchContext().getTimestampEnd() >= 0)
      filter.setEndTime(context.getSearchContext().getTimestampEnd());

    List<EventDTO> events = manager.getEvents(filter);

    Collections.sort(events, new Comparator<EventDTO>() {
      @Override
      public int compare(EventDTO o1, EventDTO o2) {
        return Long.compare(o1.getStartTime(), o2.getStartTime());
      }
    });

    Map<Entity, Double> scores = new HashMap<>();
    Map<Entity, Metadata> metadata = new HashMap<>();

    // Event URN: thirdeye:event:type:name:start
    int i = events.size();
    for(EventDTO dto : events) {
      Entity e = EventEntityUtils.entityFromDTO(dto);

      double score = i-- / (double)events.size();
      scores.put(e, score);

      EventMetadata meta = new EventMetadata(dto);
      metadata.put(e, meta);
    }

    return new PipelineResult(scores, metadata);
  }
}
