package com.linkedin.thirdeye.anomaly.events;

import com.google.common.base.Strings;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.joda.time.DateTime;

public class DefaultDeploymentEventProvider implements EventDataProvider<EventDTO> {

  private final ExternalApiQueryUtil informedQueryUtil;
  private static final String SITEOPS = "siteops";

  public DefaultDeploymentEventProvider (String informedAPIUrl) {
    informedQueryUtil = new ExternalApiQueryUtil(informedAPIUrl);
  }

  @Override
  public List<EventDTO> getEvents(EventFilter eventFilter) {
    List<EventDTO> qualifiedDeploymentEvents = new ArrayList<>();
    ExternalApiQueryUtil.InFormedPosts inFormedPosts = informedQueryUtil
        .retrieveInformedEvents(SITEOPS, new DateTime(eventFilter.getStartTime()),
            new DateTime(eventFilter.getEndTime()));
    if (inFormedPosts != null && inFormedPosts.getData() != null
        && inFormedPosts.getData().size() > 0) {
      for (ExternalApiQueryUtil.InFormedPost inFormedPost : inFormedPosts.getData()) {
        // TODO: have a better string matching once service name is extracted from content - informed API.
        if (!Strings.isNullOrEmpty(eventFilter.getServiceName()) && !inFormedPost.getContent()
            .contains(eventFilter.getServiceName())) {
          continue;
        }
        EventDTO eventDTO = getEventFromInformed(inFormedPost);
        qualifiedDeploymentEvents.add(eventDTO);
      }
    }
    return qualifiedDeploymentEvents;
  }

  private EventDTO getEventFromInformed (ExternalApiQueryUtil.InFormedPost inFormedPost) {
    EventDTO eventDTO = new EventDTO();
    eventDTO.setName(SITEOPS);
    eventDTO.setStartTime((long)(Double.parseDouble(inFormedPost.getPost_time()) * 1000));
    eventDTO.setEndTime(eventDTO.getStartTime() + TimeUnit.MINUTES.toMillis(5));
    eventDTO.setEventType(EventType.DEPLOYMENT.name());
    Map<String, List<String>> targetDimensions = new HashMap<>();
    targetDimensions.put("topic", Arrays.asList(inFormedPost.getTopic()));
    targetDimensions.put("source", Arrays.asList(inFormedPost.getSource()));
    targetDimensions.put("timestamp", Arrays.asList(inFormedPost.getTimestamp()));
    targetDimensions.put("user", Arrays.asList(inFormedPost.getUser()));
    targetDimensions.put("content", Arrays.asList(inFormedPost.getContent()));
    eventDTO.setTargetDimensionMap(targetDimensions);
    return eventDTO;
  }
}
