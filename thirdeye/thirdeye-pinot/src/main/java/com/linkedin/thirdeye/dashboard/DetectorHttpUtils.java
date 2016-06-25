package com.linkedin.thirdeye.dashboard;

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
public class DetectorHttpUtils {

  private HttpHost detectorHttpHost;

  private static String ANOMALY_JOBS_ENDPOINT = "/api/anomaly-jobs/";
  private static String EMAIL_JOBS_ENDPOINT = "/api/email-jobs/";
  private static String ADHOC = "/ad-hoc";

  public DetectorHttpUtils(String detectorHost, int detectorPort) {
    this.detectorHttpHost = new HttpHost(detectorHost, detectorPort);
  }

  public String enableAnomalyFunction(String id) throws ClientProtocolException, IOException {
    HttpPost req = new HttpPost(ANOMALY_JOBS_ENDPOINT + id);
    return callDetectorEndpoint(req);
  }

  public String disableAnomalyFunction(String id) throws ClientProtocolException, IOException {
    HttpDelete req = new HttpDelete(ANOMALY_JOBS_ENDPOINT + id);
    return callDetectorEndpoint(req);
  }

  public String runAdhocAnomalyFunction(String id, String start, String end)
      throws ClientProtocolException, IOException {
    HttpPost req = new HttpPost(ANOMALY_JOBS_ENDPOINT + id + ADHOC + "?start=" + start + "&end=" + end);
    return callDetectorEndpoint(req);
  }

  public String enableEmailConfiguration(String id) throws ClientProtocolException, IOException {
    HttpPost req = new HttpPost(EMAIL_JOBS_ENDPOINT + id);
    return callDetectorEndpoint(req);
  }

  public String disableEmailConfiguration(String id) throws ClientProtocolException, IOException {
    HttpDelete req = new HttpDelete(EMAIL_JOBS_ENDPOINT + id);
    return callDetectorEndpoint(req);
  }

  public String runAdhocEmailConfiguration(String id)
      throws ClientProtocolException, IOException {
    HttpPost req = new HttpPost(EMAIL_JOBS_ENDPOINT + id + ADHOC);
    return callDetectorEndpoint(req);
  }

  private String callDetectorEndpoint(HttpRequest req) throws IOException {
    HttpClient controllerClient = new DefaultHttpClient();
    HttpResponse res = controllerClient.execute(detectorHttpHost, req);
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
