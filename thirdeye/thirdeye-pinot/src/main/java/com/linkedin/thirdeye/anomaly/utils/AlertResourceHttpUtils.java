package com.linkedin.thirdeye.anomaly.utils;

import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

/**
 * Utility classes for calling detector endpoints to execute/schedule jobs
 */
public class AlertResourceHttpUtils {

  private HttpHost alertJobHttpHost;

  private static String ALERT_JOB_ENDPOINT = "/api/alert-job/";
  private static String ADHOC = "/ad-hoc";

  public AlertResourceHttpUtils(String alertHost, int alertPort) {
    this.alertJobHttpHost = new HttpHost(alertHost, alertPort);
  }

  public String enableEmailConfiguration(String id) throws ClientProtocolException, IOException {
    HttpPost req = new HttpPost(ALERT_JOB_ENDPOINT + id);
    return callAlertJobEndpoint(req);
  }

  public String disableEmailConfiguration(String id) throws ClientProtocolException, IOException {
    HttpDelete req = new HttpDelete(ALERT_JOB_ENDPOINT + id);
    return callAlertJobEndpoint(req);
  }

  public String runAdhocEmailConfiguration(String id, String startTimeIso, String endTimeIso)
      throws ClientProtocolException, IOException {
    HttpPost req = new HttpPost(ALERT_JOB_ENDPOINT + id + ADHOC + "?start=" + startTimeIso + "&end=" + endTimeIso);
    return callAlertJobEndpoint(req);
  }

  private String callAlertJobEndpoint(HttpRequest req) throws IOException {
    HttpClient controllerClient = new DefaultHttpClient();
    HttpResponse res = controllerClient.execute(alertJobHttpHost, req);
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
