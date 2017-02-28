package com.linkedin.thirdeye.anomaly.utils;

import java.io.IOException;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.HttpPost;


/**
 * Utility class for calling OnboardResource endpoint to execute/schedule jobs
 */
public class OnboardResourceHttpUtils extends AbstractResourceHttpUtils {
  private String Onboard_Resource_ENDPOINT = "/onboard/";
  private String Function_Key = "function";
  private String Clone_Key = "clone";

  public OnboardResourceHttpUtils(String onboardHost, int onboardPost) {
    super(new HttpHost(onboardHost, onboardPost));
  }

  public String getClonedFunctionID(String functionId, String tag, boolean isCloneAnomaly) throws IOException{
    HttpPost req = new HttpPost(Onboard_Resource_ENDPOINT + Function_Key + "/" + functionId + "/" + Clone_Key + "/"
    + tag + "?cloneAnomaly=" + String.valueOf(isCloneAnomaly));
    return callJobEndpoint(req);
  }
}
