package com.linkedin.thirdeye.anomaly.utils;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.protocol.ClientContext;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;

public abstract class AbstractResourceHttpUtils {

  private final HttpHost resourceHttpHost;
  private CookieStore cookieStore;

  protected AbstractResourceHttpUtils(HttpHost httpHost) {
    this.resourceHttpHost = httpHost;
    this.cookieStore = new BasicCookieStore();
  }

  public void addCookie(BasicClientCookie cookie) {
    cookieStore.addCookie(cookie);
  }

  public void addAuthenticationCookie(String authToken) {
    BasicClientCookie cookie = new BasicClientCookie("te_auth", authToken);
    cookie.setDomain(resourceHttpHost.getHostName());
    cookie.setPath("/");
    addCookie(cookie);
  }

  protected HttpHost getResourceHttpHost() {
    return resourceHttpHost;
  }

  protected String callJobEndpoint(HttpRequest req) throws IOException {
    HttpClient controllerClient = new DefaultHttpClient();
    HttpContext controllerContext = new BasicHttpContext();
    controllerContext.setAttribute(ClientContext.COOKIE_STORE, cookieStore);
    HttpResponse res = controllerClient.execute(resourceHttpHost, req, controllerContext);
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
