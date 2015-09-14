package com.linkedin.thirdeye.reporting.util;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.client.ThirdEyeRawResponse;

public class ThirdeyeServerUtils {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();


  public static ThirdEyeRawResponse getReport(String serverUri, String sql) throws IOException {

    URL url = new URL(serverUri + "/query/" + URLEncoder.encode(sql, "UTF-8"));
    return OBJECT_MAPPER.readValue((new InputStreamReader(url.openStream(), "UTF-8")), ThirdEyeRawResponse.class);
  }

  public static StarTreeConfig getStarTreeConfig(String serverUri, String collection) throws JsonParseException, JsonMappingException, UnsupportedEncodingException, IOException {

    URL url = new URL(serverUri + "/collections/" + collection);
    return OBJECT_MAPPER.readValue((new InputStreamReader(url.openStream(), "UTF-8")), StarTreeConfig.class);
  }

}
