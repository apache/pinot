package com.linkedin.thirdeye;

import com.linkedin.thirdeye.api.ThirdEyeMetrics;
import com.linkedin.thirdeye.api.ThirdEyeTimeSeries;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.WebResource;

import java.net.InetSocketAddress;
import java.util.List;

public class ThirdEyeClientJerseyImpl implements ThirdEyeClient
{
  private static final GenericType<List<ThirdEyeMetrics>> METRICS_LIST
          = new GenericType<List<ThirdEyeMetrics>>(){};
  private static final GenericType<List<ThirdEyeTimeSeries>> TIME_SERIES_LIST
          = new GenericType<List<ThirdEyeTimeSeries>>(){};

  private final InetSocketAddress socketAddress;
  private final Client client;

  public ThirdEyeClientJerseyImpl(InetSocketAddress socketAddress)
  {
    this.socketAddress = socketAddress;
    this.client = Client.create();
  }

  @Override
  public List<ThirdEyeMetrics> getMetrics(ThirdEyeQuery query)
  {
    WebResource resource = client.resource(getFullUri(query.getMetricsPath()));
    return resource.get(METRICS_LIST);
  }

  @Override
  public List<ThirdEyeTimeSeries> getTimeSeries(ThirdEyeQuery query)
  {
    WebResource resource = client.resource(getFullUri(query.getTimeSeriesPath()));
    return resource.get(TIME_SERIES_LIST);
  }

  private String getFullUri(String path)
  {
    return String.format("http://%s:%d%s", socketAddress.getHostName(), socketAddress.getPort(), path);
  }
}
