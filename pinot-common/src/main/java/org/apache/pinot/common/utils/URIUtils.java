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
package org.apache.pinot.common.utils;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Map;
import java.util.StringJoiner;
import org.apache.http.client.utils.URIBuilder;
import org.apache.pinot.spi.utils.CommonConstants;


public class URIUtils {
  private URIUtils() {
  }

  /**
   * Returns the URI for the given path, appends the local (file) scheme to the URI if no scheme exists.
   */
  public static URI getUri(String path) {
    try {
      URI uri = new URI(path);
      if (uri.getScheme() != null) {
        return uri;
      } else {
        return new URI(CommonConstants.Segment.LOCAL_SEGMENT_SCHEME + ":" + path);
      }
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Illegal URI path: " + path, e);
    }
  }

  /**
   * Returns the URI for the given base path and optional parts, appends the local (file) scheme to the URI if no
   * scheme exists. All the parts will be appended to the base path with the file separator.
   */
  public static URI getUri(String basePath, String... parts) {
    return getUri(getPath(basePath, parts));
  }

  /**
   * Returns the path for the given base path and optional parts. All the parts will be appended to the base path with
   * the file separator.
   */
  public static String getPath(String basePath, String... parts) {
    StringJoiner stringJoiner = new StringJoiner(File.separator);
    stringJoiner.add(basePath);
    for (String part : parts) {
      stringJoiner.add(part);
    }
    return stringJoiner.toString();
  }

  /**
   * Returns the last part for the given path split by the file separator.
   * If the file separator is not found, returns the whole path as the last part.
   */
  public static String getLastPart(String path) {
    if (path == null) {
      return null;
    }
    int parameterIndex = path.indexOf("?");
    path = parameterIndex >= 0 ? path.substring(0, parameterIndex) : path;
    return path.substring(path.lastIndexOf(File.separator) + 1);
  }

  /**
   * Returns the download URL with the segment name encoded.
   */
  public static String constructDownloadUrl(String baseUrl, String rawTableName, String segmentName) {
    return getPath(baseUrl, "segments", rawTableName, encode(segmentName));
  }

  public static String encode(String string) {
    try {
      return URLEncoder.encode(string, "UTF-8");
    } catch (Exception e) {
      // Should never happen
      throw new RuntimeException(e);
    }
  }

  public static String decode(String string) {
    try {
      return URLDecoder.decode(string, "UTF-8");
    } catch (Exception e) {
      // Should never happen
      throw new RuntimeException(e);
    }
  }

  /**
   * Builds the URI using the schema, host, port, path and map of params.
   * The URI builder automatically encodes fields as needed
   */
  public static URI buildURI(String schema, String hostPort, String path, Map<String, String> params) {
    URIBuilder uriBuilder = new URIBuilder().setScheme(schema).setHost(hostPort).setPath(path);
    for (Map.Entry<String, String> entry : params.entrySet()) {
      uriBuilder.addParameter(entry.getKey(), entry.getValue());
    }
    try {
      return uriBuilder.build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Builds the URI using the schema, host, port, path and map of params.
   * The URI builder automatically encodes fields as needed
   */
  public static URI buildURI(String schema, String host, int port, String path, Map<String, String> params) {
    return buildURI(schema, String.format("%s:%d", host, port), path, params);
  }
}
