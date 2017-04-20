package com.linkedin.thirdeye.rootcause.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;


import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.rootcause.Entity;


/**
 * EventEntity represents an individual event. It holds meta-data referencing ThirdEye's internal
 * database. The URN namespace is defined as 'thirdeye:event:{type}:{name}:{start}'.
 */
public class EventEntity extends Entity {
  private static final EntityType TYPE = new EntityType("thirdeye:event:");

  public static EventEntity fromDTO(double score, EventDTO dto) {
    String urn = TYPE.formatURN(dto.getEventType(), dto.getName(), dto.getStartTime());
    return new EventEntity(urn, score, dto);
  }

  final EventDTO dto;

  protected EventEntity(String urn, double score, EventDTO dto) {
    super(urn, score);
    this.dto = dto;
  }

  public EventDTO getDto() {
    return dto;
  }

  public static List<EventDTO> applyDimensionFilter(List<EventDTO> allEvents, Map<String, List<String>> eventFilterDimensionMap) {
    List<EventDTO> filteredEvents = new ArrayList<>();

    if (CollectionUtils.isNotEmpty(allEvents)) {

      // if filter map not empty, filter events
      if (MapUtils.isNotEmpty(eventFilterDimensionMap)) {

        // go over each event
        for (EventDTO event : allEvents) {
          boolean eventAdded = false;
          Map<String, List<String>> eventDimensionMap = event.getTargetDimensionMap();

          // if dimension map is empty, this event will be skipped, because we know that event filter is not empty
          if (MapUtils.isNotEmpty(eventDimensionMap)) {

            // go over each dimension in event's dimension map, to see if it passes any filter
            for (Entry<String, List<String>> eventMapEntry : eventDimensionMap.entrySet()) {
              String eventDimension = eventMapEntry.getKey();
              String eventDimensionTransformed = eventDimension.toLowerCase().replaceAll("[^A-Za-z0-9]", "");
              List<String> eventDimensionValues = eventMapEntry.getValue();

              // for each filter_dimension : dimension_values pair
              for (Entry<String, List<String>> filterMapEntry : eventFilterDimensionMap.entrySet()) {
                String filterDimension = filterMapEntry.getKey();
                String filterDimensionTransformed = filterDimension.toLowerCase().replaceAll("[^A-Za-z0-9]", "");
                List<String> filterDimensionValues = filterMapEntry.getValue();

                // if event has this dimension to filter on
                if (eventDimensionTransformed.contains(filterDimensionTransformed) ||
                    filterDimensionTransformed.contains(eventDimensionTransformed)) {
                  // and if it matches any of the filter values, add it
                  Set<String> eventDimensionValuesSet = new HashSet<>(eventDimensionValues);
                  eventDimensionValuesSet.retainAll(filterDimensionValues);
                  if (!eventDimensionValuesSet.isEmpty()) {
                    filteredEvents.add(event);
                    eventAdded = true;
                    break;
                  }
                }
              }
              if (eventAdded) {
                break;
              }
            }
          }
        }
      } else {
        filteredEvents.addAll(allEvents);
      }
    }
    return filteredEvents;
  }
}
