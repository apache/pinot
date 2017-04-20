package com.linkedin.thirdeye.anomaly.events;

import com.linkedin.thirdeye.client.DAORegistry;
import com.linkedin.thirdeye.datalayer.bao.EventManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

public class DefaultHolidayEventProvider implements EventDataProvider<EventDTO> {
  private EventManager eventDAO = DAORegistry.getInstance().getEventDAO();

  @Override
  public List<EventDTO> getEvents(EventFilter eventFilter) {
    List<EventDTO> allEventsBetweenTimeRange =
        eventDAO.findEventsBetweenTimeRange(EventType.HOLIDAY.name(), eventFilter.getStartTime(),
            eventFilter.getEndTime());

    List<EventDTO> holidayEvents = applyDimensionFilter(allEventsBetweenTimeRange, eventFilter.getTargetDimensionMap());
    return holidayEvents;
  }


  public List<EventDTO> applyDimensionFilter(List<EventDTO> allEvents, Map<String, List<String>> eventFilterDimensionMap) {
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
              // TODO: get this transformation from standardization table
              String eventDimension = eventMapEntry.getKey();
              String eventDimensionTransformed = transformDimensionName(eventDimension);
              List<String> eventDimensionValues = eventMapEntry.getValue();
              List<String> eventDimensionValuesTransformed = transformDimensionValues(eventDimensionValues);

              // for each filter_dimension : dimension_values pair
              for (Entry<String, List<String>> filterMapEntry : eventFilterDimensionMap.entrySet()) {
                // TODO: get this transformation from standardization table
                String filterDimension = filterMapEntry.getKey();
                String filterDimensionTransformed = transformDimensionName(filterDimension);
                List<String> filterDimensionValues = filterMapEntry.getValue();
                List<String> filteredDimensionValuesTransformed = transformDimensionValues(filterDimensionValues);

                // if event has this dimension to filter on
                if (eventDimensionTransformed.contains(filterDimensionTransformed) ||
                    filterDimensionTransformed.contains(eventDimensionTransformed)) {
                  // and if it matches any of the filter values, add it
                  Set<String> eventDimensionValuesSet = new HashSet<>(eventDimensionValuesTransformed);
                  eventDimensionValuesSet.retainAll(filteredDimensionValuesTransformed);
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

  private String transformDimensionName(String dimensionName) {
    String dimensionNameTransformed = dimensionName.toLowerCase().replaceAll("[^A-Za-z0-9]", "");
    return dimensionNameTransformed;
  }

  private List<String> transformDimensionValues(List<String> dimensionValues) {
    List<String> dimensionValuesTransformed = new ArrayList<>();
    for (String value : dimensionValues) {
      dimensionValuesTransformed.add(value.toLowerCase());
    }
    return dimensionValuesTransformed;
  }
}
