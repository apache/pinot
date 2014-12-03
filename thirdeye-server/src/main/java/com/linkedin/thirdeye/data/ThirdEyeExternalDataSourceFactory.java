package com.linkedin.thirdeye.data;

import com.linkedin.thirdeye.ThirdEyeApplication;
import io.dropwizard.client.HttpClientBuilder;
import io.dropwizard.setup.Environment;
import org.apache.http.HttpHost;
import org.apache.http.client.HttpClient;

import java.net.URI;

public class ThirdEyeExternalDataSourceFactory
{
  public ThirdEyeExternalDataSource createExternalDataSource(URI sourceUri,
                                                             ThirdEyeApplication.Config config,
                                                             Environment environment)
  {
    if ("http".equals(sourceUri.getScheme()))
    {
      HttpClient httpClient
              = new HttpClientBuilder(environment).using(config.getHttpClientConfiguration())
                                                  .build("httpClient");

      return new ThirdEyeExternalDataSourceHttpImpl(httpClient,
                                                    new HttpHost(sourceUri.getHost(), sourceUri.getPort()));
    }
    else
    {
      throw new IllegalArgumentException("Invalid URI scheme " + sourceUri.getScheme());
    }
  }
}
