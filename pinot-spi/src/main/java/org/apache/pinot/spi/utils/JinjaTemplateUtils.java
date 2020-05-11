/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spi.utils;

import com.hubspot.jinjava.Jinjava;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;


public class JinjaTemplateUtils {

  private static final Jinjava JINJAVA = new Jinjava();
  private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

  public static String renderTemplate(String template, Map<String, Object> newContext) {
    Map<String, Object> contextMap = getDefaultJinjaContextMap();
    contextMap.putAll(newContext);
    return JINJAVA.render(template, contextMap);
  }

  /**
   Construct default template context:
   today : today's date in format `yyyy-MM-dd`, example value: '2020-05-06'
   yesterday : yesterday's date in format `yyyy-MM-dd`, example value: '2020-05-06'
   */
  public static Map<String, Object> getDefaultJinjaContextMap() {
    Map<String, Object> defaultJinjaContextMap = new HashMap<>();
    Instant now = Instant.now();
    defaultJinjaContextMap.put("today", DATE_FORMAT.format(new Date(now.toEpochMilli())));
    defaultJinjaContextMap.put("yesterday", DATE_FORMAT.format(new Date(now.minus(1, ChronoUnit.DAYS).toEpochMilli())));
    return defaultJinjaContextMap;
  }

  public static Map<String, Object> getTemplateContext(List<String> values) {
    Map<String, Object> context = new HashMap<>();
    for (String value : values) {
      String[] splits = value.split("=", 2);
      if (splits.length > 1) {
        context.put(splits[0], splits[1]);
      }
    }
    return context;
  }

  public static String renderTemplate(String template) {
    return renderTemplate(template, Collections.emptyMap());
  }

  static {
    DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
  }
}
