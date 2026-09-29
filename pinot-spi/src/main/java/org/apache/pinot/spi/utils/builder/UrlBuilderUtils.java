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
package org.apache.pinot.spi.utils.builder;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.collections4.CollectionUtils;


public class UrlBuilderUtils {
  private UrlBuilderUtils() {
  }

  @Nullable
  public static String generateColumnsParam(@Nullable List<String> columns) {
    if (CollectionUtils.isEmpty(columns)) {
      return null;
    }
    StringBuilder builder = new StringBuilder("columns=").append(encode(columns.get(0)));
    int numColumns = columns.size();
    for (int i = 1; i < numColumns; i++) {
      builder.append("&columns=").append(encode(columns.get(i)));
    }
    return builder.toString();
  }

  @Nullable
  public static String generateSegmentsParam(@Nullable List<String> segments) {
    if (CollectionUtils.isEmpty(segments)) {
      return null;
    }
    StringBuilder builder = new StringBuilder("segments=").append(encode(segments.get(0)));
    int numSegments = segments.size();
    for (int i = 1; i < numSegments; i++) {
      builder.append("&segments=").append(encode(segments.get(i)));
    }
    return builder.toString();
  }

  public static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  @Nullable
  public static String generateColumnsAndSegmentsParam(@Nullable List<String> columns,
      @Nullable List<String> segments) {
    String columnsParam = generateColumnsParam(columns);
    String segmentsParam = generateSegmentsParam(segments);
    if (columnsParam == null) {
      return segmentsParam;
    }
    if (segmentsParam == null) {
      return columnsParam;
    }
    return columnsParam + "&" + segmentsParam;
  }
}
