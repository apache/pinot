package com.linkedin.thirdeye.anomaly.utils;

import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

public abstract class AbstractResourceHttpUtils {

  private final HttpHost resourceHttpHost;

  protected AbstractResourceHttpUtils(HttpHost httpHost) {
    this.resourceHttpHost = httpHost;
  }

  protected HttpHost getResourceHttpHost() {
    return resourceHttpHost;
  }

  protected String callJobEndpoint(HttpRequest req) throws IOException {
    HttpClient controllerClient = new DefaultHttpClient();
    HttpResponse res = controllerClient.execute(resourceHttpHost, req);
    String response = null;
    try {
      if (res.getStatusLine().getStatusCode() != 200) {
        throw new IllegalStateException(res.getStatusLine().toString());
      }
      InputStream content = res.getEntity().getContent();
      response = IOUtils.toString(content);

    } finally {
      if (res.getEntity() != null) {
        EntityUtils.consume(res.getEntity());
      }
    }
    return response;
  }
}
