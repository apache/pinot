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

import groovy.text.SimpleTemplateEngine;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;


public class GroovyTemplateUtils {
  private GroovyTemplateUtils() {
  }

  private static final SimpleTemplateEngine GROOVY_TEMPLATE_ENGINE = new SimpleTemplateEngine();
  private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

  public static String renderTemplate(String template, Map<String, Object> newContext)
      throws IOException, ClassNotFoundException {
    Map<String, Object> contextMap = getDefaultContextMap();
    contextMap.putAll(newContext);
    return GROOVY_TEMPLATE_ENGINE.createTemplate(template).make(contextMap).toString();
  }

  /**
   Construct default template context:
   today : today's date in format `yyyy-MM-dd`, example value: '2020-05-06'
   yesterday : yesterday's date in format `yyyy-MM-dd`, example value: '2020-05-06'
   */
  public static Map<String, Object> getDefaultContextMap() {
    Map<String, Object> defaultContextMap = new HashMap<>();
    Instant now = Instant.now();
    defaultContextMap.put("today", DATE_FORMAT.format(new Date(now.toEpochMilli())));
    defaultContextMap.put("yesterday", DATE_FORMAT.format(new Date(now.minus(1, ChronoUnit.DAYS).toEpochMilli())));
    return defaultContextMap;
  }

  public static Map<String, Object> getTemplateContext(List<String> values) {
    if (values == null) {
      return Collections.emptyMap();
    }
    Map<String, Object> context = new HashMap<>();
    for (String value : values) {
      String[] splits = value.split("=", 2);
      if (splits.length > 1) {
        context.put(splits[0], splits[1]);
      }
    }
    return context;
  }

  public static String renderTemplate(String template)
      throws IOException, ClassNotFoundException {
    return renderTemplate(template, Collections.emptyMap());
  }

  static {
    DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    GROOVY_TEMPLATE_ENGINE.setEscapeBackslash(true);
  }
}
