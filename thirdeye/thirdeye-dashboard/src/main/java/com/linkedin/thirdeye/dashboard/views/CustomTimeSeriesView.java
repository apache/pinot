package com.linkedin.thirdeye.dashboard.views;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.dropwizard.views.View;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CustomTimeSeriesView extends View {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Map<String, List<Number[]>> data;

  public CustomTimeSeriesView(Map<String, List<Number[]>> data) {
    super("custom-time-series.ftl"); // does not exist, not stand alone now
    this.data = data;
  }

  public String getJsonString() throws IOException {
    List<Map<String, Object>> json = new ArrayList<>();

    for (Map.Entry<String, List<Number[]>> entry : data.entrySet()) {
      json.add(ImmutableMap.of("label", entry.getKey(), "data", entry.getValue()));
    }

    return OBJECT_MAPPER.writeValueAsString(json);
  }
}
