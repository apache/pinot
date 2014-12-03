package com.linkedin.thirdeye.data;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;

public class ThirdEyeExternalDataSourceHttpImpl implements ThirdEyeExternalDataSource
{
  private final HttpClient httpClient;
  private final HttpHost httpHost;

  public ThirdEyeExternalDataSourceHttpImpl(HttpClient httpClient, HttpHost httpHost)
  {
    this.httpClient = httpClient;
    this.httpHost = httpHost;
  }

  @Override
  public void copy(URI archiveUri, OutputStream outputStream) throws IOException
  {
    HttpRequest req = new HttpGet(archiveUri);
    HttpResponse res = httpClient.execute(httpHost, req);
    IOUtils.copy(res.getEntity().getContent(), outputStream);
    EntityUtils.consume(res.getEntity());
  }
}
