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
public class DetectionResourceHttpUtils {

  private HttpHost detectionResourceHttpHost;

  private static String DETECTION_JOB_ENDPOINT = "/api/detection-job/";
  private static String ADHOC = "/ad-hoc";

  public DetectionResourceHttpUtils(String detectionHost, int detectionPort) {
    this.detectionResourceHttpHost = new HttpHost(detectionHost, detectionPort);
  }

  public String enableAnomalyFunction(String id) throws ClientProtocolException, IOException {
    HttpPost req = new HttpPost(DETECTION_JOB_ENDPOINT + id);
    return callDetectionJobEndpoint(req);
  }

  public String disableAnomalyFunction(String id) throws ClientProtocolException, IOException {
    HttpDelete req = new HttpDelete(DETECTION_JOB_ENDPOINT + id);
    return callDetectionJobEndpoint(req);
  }

  public String runAdhocAnomalyFunction(String id, String startTimeIso, String endTimeIso)
      throws ClientProtocolException, IOException {
    HttpPost req = new HttpPost(DETECTION_JOB_ENDPOINT + id + ADHOC + "?start=" + startTimeIso + "&end=" + endTimeIso);
    return callDetectionJobEndpoint(req);
  }


  private String callDetectionJobEndpoint(HttpRequest req) throws IOException {
    HttpClient controllerClient = new DefaultHttpClient();
    HttpResponse res = controllerClient.execute(detectionResourceHttpHost, req);
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
