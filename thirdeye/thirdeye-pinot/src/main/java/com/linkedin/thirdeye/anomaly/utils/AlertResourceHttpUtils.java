package com.linkedin.thirdeye.anomaly.utils;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.cookie.BasicClientCookie;


/**
 * Utility classes for calling detector endpoints to execute/schedule jobs
 */
public class AlertResourceHttpUtils extends AbstractResourceHttpUtils {

  private static String ALERT_JOB_ENDPOINT = "/api/alert-job/";
  private static String ADHOC = "/ad-hoc";

  public AlertResourceHttpUtils(String alertHost, int alertPort, String authToken) {
    super(new HttpHost(alertHost, alertPort));
    addAuthenticationCookie(authToken);
  }

  public String enableEmailConfiguration(String id) throws ClientProtocolException, IOException {
    HttpPost req = new HttpPost(ALERT_JOB_ENDPOINT + id);
    return callJobEndpoint(req);
  }

  public String disableEmailConfiguration(String id) throws ClientProtocolException, IOException {
    HttpDelete req = new HttpDelete(ALERT_JOB_ENDPOINT + id);
    return callJobEndpoint(req);
  }

  public String runAdhocEmailConfiguration(String id, String startTimeIso, String endTimeIso)
      throws ClientProtocolException, IOException {
    HttpPost req = new HttpPost(ALERT_JOB_ENDPOINT + id + ADHOC + "?start=" + startTimeIso + "&end=" + endTimeIso);
    return callJobEndpoint(req);
  }

}
