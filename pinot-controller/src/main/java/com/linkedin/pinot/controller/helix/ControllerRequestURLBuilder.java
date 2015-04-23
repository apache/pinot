/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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

  public String forDataFileUpload() {
    return StringUtil.join("/", StringUtils.chomp(base, "/"), "datafiles");
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
