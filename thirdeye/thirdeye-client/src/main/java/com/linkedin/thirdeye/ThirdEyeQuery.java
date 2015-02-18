package com.linkedin.thirdeye;

import com.google.common.base.Joiner;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ThirdEyeQuery
{
  private static final Joiner COMMA_JOINER = Joiner.on(",");
  private static final Joiner EQUALS_JOINER = Joiner.on("=");
  private static final Joiner AND_JOINER = Joiner.on("&");
  private static final String ENCODING = "UTF-8";

  private final String collection;
  private final long startTime;
  private final long endTime;
  private final Map<String, String> dimensions;
  private final Set<String> metrics;

  private ThirdEyeQuery(String collection,
                        long startTime,
                        long endTime,
                        Map<String, String> dimensions,
                        Set<String> metrics)
  {
    this.collection = collection;
    this.startTime = startTime;
    this.endTime = endTime;
    this.dimensions = dimensions;
    this.metrics = metrics;
  }

  protected String getMetricsPath()
  {
    return addDimensionValues(String.format("/metrics/%s/%d/%d", collection, startTime, endTime));
  }

  protected String getTimeSeriesPath()
  {
    if (metrics.isEmpty())
    {
      throw new IllegalArgumentException("Must provide >= 1 metric for time series query");
    }

    return addDimensionValues(
            String.format("/timeSeries/raw/%s/%s/%d/%d", collection, COMMA_JOINER.join(metrics), startTime, endTime));
  }

  private String addDimensionValues(String base)
  {
    if (dimensions.isEmpty())
    {
      return base;
    }

    List<String> queryComponents = new ArrayList<String>(dimensions.size());

    for (Map.Entry<String, String> entry : dimensions.entrySet())
    {
      queryComponents.add(EQUALS_JOINER.join(entry.getKey(), entry.getValue()));
    }

    return base + "?" + AND_JOINER.join(queryComponents);
  }

  public static class Builder
  {
    private String collection;
    private long startTime;
    private long endTime;
    private final Map<String, String> dimensions = new HashMap<String, String>();
    private final Set<String> metrics = new HashSet<String>();

    private Builder() {}

    public Builder setCollection(String collection)
    {
      this.collection = collection;
      return this;
    }

    public Builder setStartTime(long startTime)
    {
      this.startTime = startTime;
      return this;
    }

    public Builder setEndTime(long endTime)
    {
      this.endTime = endTime;
      return this;
    }

    public Builder setDimensionValue(String dimensionName, String dimensionValue)
    {
      this.dimensions.put(dimensionName, dimensionValue);
      return this;
    }

    public Builder addMetric(String metricName)
    {
      this.metrics.add(metricName);
      return this;
    }

    public ThirdEyeQuery build() throws IOException
    {
      String encodedCollection = URLEncoder.encode(collection, ENCODING);

      Set<String> encodedMetrics = new HashSet<String>(metrics.size());
      for (String metricName : metrics)
      {
        encodedMetrics.add(URLEncoder.encode(metricName, ENCODING));
      }

      Map<String, String> encodedDimensions = new HashMap<String, String>(dimensions.size());
      for (Map.Entry<String, String> entry : dimensions.entrySet())
      {
        encodedDimensions.put(URLEncoder.encode(entry.getKey(), ENCODING),
                              URLEncoder.encode(entry.getValue(), ENCODING));
      }

      return new ThirdEyeQuery(encodedCollection, startTime, endTime, encodedDimensions, encodedMetrics);
    }
  }

  public static Builder builder()
  {
    return new Builder();
  }
}
