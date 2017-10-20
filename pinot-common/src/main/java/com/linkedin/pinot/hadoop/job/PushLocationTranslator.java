/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.hadoop.job;

import com.google.common.base.Splitter;
import com.linkedin.pinot.common.utils.StringUtil;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.StringUtils;


public class PushLocationTranslator {

  private static final String SEGMENTS = "segments";
  private static final String OFFLINETABLE = "?type=OFFLINE";
  private static final String TABLES = "tables";
  private static final String SLASH = "/";
  private static final String SCHEMA = "/schema";

  public PushLocationTranslator() {
  }

  public List<String> getSegmentPostUris(String input) {
    List<String> segmentPostUris = new ArrayList<>();
    for (String baseUrl : getBaseUrls(input)) {
      segmentPostUris.add(StringUtil.join("/", StringUtils.chomp(baseUrl, SLASH), SEGMENTS));
    }
    return segmentPostUris;
  }

  public List<String> getTableConfigUris(String input, String tableName) {
    List<String> tableConfigUrls = new ArrayList<>();
    for (String baseUrl : getBaseUrls(input)) {
      tableConfigUrls.add(StringUtil.join("/", StringUtils.chomp(baseUrl, SLASH), TABLES, tableName + OFFLINETABLE));
    }
    return tableConfigUrls;
  }

  public List<String> getSchemaUrls(String input, String tableName) {
    List<String> schemaUrls = new ArrayList<>();
    for (String baseUrl : getBaseUrls(input)) {
      schemaUrls.add(StringUtil.join("/", StringUtils.chomp(baseUrl, SLASH), TABLES, tableName + SCHEMA));
    }
    return schemaUrls;
  }

  public List<String> getBaseUrls(String input) {
    Iterable<String> splitLocations = Splitter.on(',').omitEmptyStrings().trimResults().split(input);
    ArrayList<String> urlsToReturn = new ArrayList<>();

    for (String location : splitLocations) {
      // Manually specified host:port combinations
      if (location.contains(":")) {
        urlsToReturn.add("http://" + location);
      }

      if (urlsToReturn.isEmpty()) {
        throw new RuntimeException(
            "No valid locations specified. Currently specified: " + input);
      }
    }
    return urlsToReturn;
  }
}
