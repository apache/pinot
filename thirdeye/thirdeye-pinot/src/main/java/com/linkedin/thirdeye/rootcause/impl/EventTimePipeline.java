package com.linkedin.thirdeye.rootcause.impl;

import com.linkedin.thirdeye.anomaly.events.EventDataProviderManager;
import com.linkedin.thirdeye.anomaly.events.EventFilter;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.Entity;
import com.linkedin.thirdeye.rootcause.ExecutionContext;
import com.linkedin.thirdeye.rootcause.Pipeline;
import com.linkedin.thirdeye.rootcause.PipelineResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EventTimePipeline implements Pipeline {
  private static Logger LOG = LoggerFactory.getLogger(EventTimePipeline.class);

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
    TimeRangeEntity current = EntityUtils.getContextTimeRange(context);
    if(current == null) {
      LOG.warn("Pipeline '{}' requires TimeRangeEntity. Skipping.", this.getName());
      return new PipelineResult(Collections.<Entity>emptyList());
    }

    EventFilter filter = new EventFilter();
    filter.setStartTime(current.getStart());
    filter.setEndTime(current.getEnd());

    List<EventDTO> events = provider.getEvents(filter);

    List<EventEntity> entities = new ArrayList<>();
    for(EventDTO e : events) {
      long distance = current.getEnd() - e.getStartTime();
      double score = -distance;
      entities.add(EventEntity.fromDTO(score, e));
    }

    return new PipelineResult(entities);
  }
}
