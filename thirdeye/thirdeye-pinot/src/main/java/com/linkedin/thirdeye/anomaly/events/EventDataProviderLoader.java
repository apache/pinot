package com.linkedin.thirdeye.anomaly.events;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.RCAConfiguration;

public class EventDataProviderLoader {

  private static final Logger LOG = LoggerFactory.getLogger(EventDataProviderLoader.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(new YAMLFactory());

  public static void registerEventDataProvidersFromConfig(File rcaConfig, EventDataProviderManager eventProvider) {

    try {
      RCAConfiguration rcaConfiguration = OBJECT_MAPPER.readValue(rcaConfig, RCAConfiguration.class);
      List<EventDataProviderConfiguration> eventDataProvidersConfiguration = rcaConfiguration.getEventDataProvidersConfiguration();
      if (CollectionUtils.isNotEmpty(eventDataProvidersConfiguration)) {
        for (EventDataProviderConfiguration eventDataProviderConfig : eventDataProvidersConfiguration) {
          String name = eventDataProviderConfig.getName();
          String className = eventDataProviderConfig.getClassName();
          Map<String, String> properties = eventDataProviderConfig.getProperties();

          Constructor<?> constructor = Class.forName(className).getConstructor(String.class, Map.class);
          EventDataProvider<EventDTO> eventDataProvider = (EventDataProvider) constructor.newInstance(name, properties);
          eventProvider.registerEventDataProvider(name, eventDataProvider);
        }
      }
    } catch (Exception e) {
      LOG.error("Exception in loading rca configs", e);
    }
  }

  public static void main(String[] args) {
    // External event providers
    File rcaConfig = new File("/Users/npawar/pinotAbookInstance/dashboard-controller/rca.yml");
    if (rcaConfig.exists()) {
      EventDataProviderLoader.registerEventDataProvidersFromConfig(rcaConfig, EventDataProviderManager.getInstance());
      EventFilter ef = new EventFilter();
      ef.setEventType("INFORMED");
      List<EventDTO> events = EventDataProviderManager.getInstance().getEvents(ef);
      for (EventDTO event : events) {
        System.out.println(event);
      }
    }

  }

}
