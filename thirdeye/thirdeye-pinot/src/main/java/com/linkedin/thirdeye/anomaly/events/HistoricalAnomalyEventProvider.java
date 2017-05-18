package com.linkedin.thirdeye.anomaly.events;

import com.linkedin.thirdeye.datalayer.bao.MergedAnomalyResultManager;
import com.linkedin.thirdeye.datalayer.dto.EventDTO;
import com.linkedin.thirdeye.datalayer.dto.MergedAnomalyResultDTO;
import com.linkedin.thirdeye.datasource.DAORegistry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HistoricalAnomalyEventProvider implements EventDataProvider<EventDTO> {

  private MergedAnomalyResultManager mergedAnomalyDAO = DAORegistry.getInstance().getMergedAnomalyResultDAO();

  @Override
  public List<EventDTO> getEvents(EventFilter eventFilter) {
    List<MergedAnomalyResultDTO> mergedAnomalies;
    List<EventDTO> events = new ArrayList<>();
    if (eventFilter.getMetricName() == null) {
      mergedAnomalies =
          mergedAnomalyDAO.findByTime(eventFilter.getStartTime(), eventFilter.getStartTime(), false);
    } else {
      mergedAnomalies = mergedAnomalyDAO
          .findByMetricTime(eventFilter.getMetricName(), eventFilter.getStartTime(),
              eventFilter.getStartTime(), false);
    }

    if (mergedAnomalies != null) {
      for(MergedAnomalyResultDTO mergedAnomalyResultDTO : mergedAnomalies) {
        events.add(getAnomalyEvent(mergedAnomalyResultDTO));
      }
    }

    return events;
  }

  private EventDTO getAnomalyEvent(MergedAnomalyResultDTO anomalyResultDTO) {
    EventDTO eventDTO = new EventDTO();
    eventDTO.setEventType(EventType.HISTORICAL_ANOMALY.name());
    eventDTO.setStartTime(anomalyResultDTO.getStartTime());
    eventDTO.setEndTime(anomalyResultDTO.getEndTime());
    eventDTO.setMetric(anomalyResultDTO.getMetric());
    eventDTO.setId(anomalyResultDTO.getId());
    Map<String, List<String>> dimensionValuesMap = new HashMap<>();
    if (anomalyResultDTO.getDimensions() != null && anomalyResultDTO.getDimensions().size() > 0) {
      for (String dimension : anomalyResultDTO.getDimensions().keySet()) {
        List<String> dimensionValues;
        if (dimensionValuesMap.containsKey(dimension)) {
          dimensionValues = dimensionValuesMap.get(dimension);
        } else {
          dimensionValues = new ArrayList<>();
          dimensionValuesMap.put(dimension, dimensionValues);
        }
        dimensionValues.add(anomalyResultDTO.getDimensions().get(dimension));
      }
    }
    eventDTO.setTargetDimensionMap(dimensionValuesMap);
    return eventDTO;
  }

  @Override
  public String getEventType() {
    return EventType.HISTORICAL_ANOMALY.toString();
  }

}
