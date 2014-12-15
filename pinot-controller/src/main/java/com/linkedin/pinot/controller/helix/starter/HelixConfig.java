package com.linkedin.pinot.controller.helix.starter;

import org.apache.commons.lang.StringUtils;


public class HelixConfig {
  public static final String PINOT_RESOURCE_MANAGER = "HelixResourceManager";

  public static String getAbsoluteZkPathForHelix(String zkBaseUrl) {
    zkBaseUrl = StringUtils.chomp(zkBaseUrl, "/");
    return zkBaseUrl;
  }
}
