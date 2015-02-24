package com.linkedin.thirdeye.views;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.timeseries.FlotTimeSeries;
import io.dropwizard.views.View;

import java.io.IOException;
import java.util.List;

public class TimeSeriesComponentView extends View
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final List<FlotTimeSeries> flotSeries;

  public TimeSeriesComponentView(List<FlotTimeSeries> flotSeries)
  {
    super("time-series-component.ftl");
    this.flotSeries = flotSeries;
  }

  public String getFlotJsonData() throws IOException
  {
    return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(flotSeries);
  }
}
