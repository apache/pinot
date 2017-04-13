package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.anomaly.events.EventDataProviderManager;
import com.linkedin.thirdeye.anomaly.events.EventFilter;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.ExecutionContext;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class EventTimePipeline implements Pipeline {
  final EventDataProviderManager provider;

  public EventTimePipeline(EventDataProviderManager manager) {
    this.provider = manager;
  }

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

    List<EventDTO> events = provider.getEvents(filter);

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
