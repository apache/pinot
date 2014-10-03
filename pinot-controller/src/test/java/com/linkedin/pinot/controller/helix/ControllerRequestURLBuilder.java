package com.linkedin.pinot.controller.helix;

import org.apache.commons.lang.StringUtils;

import com.linkedin.pinot.common.utils.StringUtil;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Sep 30, 2014
 */

public class ControllerRequestURLBuilder {
  public static String base;

  public static ControllerRequestURLBuilder baseUrl(String baseUrl) {
    base = baseUrl;
    return new ControllerRequestURLBuilder();
  }

  public String forResourceCreate() {
    return StringUtil.join("/", StringUtils.chomp(base, "/"), "dataresources");
  }

  public String forResourceDelete(String resourceName) {
    return StringUtil.join("/", StringUtils.chomp(base, "/"), "dataresources", resourceName);
  }

  public String forResourceGet(String resourceName) {
    return StringUtil.join("/", StringUtils.chomp(base, "/"), "dataresources", resourceName);
  }

  public String forInstanceCreate() {
    return StringUtil.join("/", StringUtils.chomp(base, "/"), "instances/");
  }

  public String forInstanceBulkCreate() {
    return StringUtil.join("/", StringUtils.chomp(base, "/"), "instances", "bulkAdd");
  }

  public static void main(String[] args) {
    System.out.println(ControllerRequestURLBuilder.baseUrl("localhost:8089").forResourceCreate());
    System.out.println(ControllerRequestURLBuilder.baseUrl("localhost:8089").forInstanceCreate());
  }
}
