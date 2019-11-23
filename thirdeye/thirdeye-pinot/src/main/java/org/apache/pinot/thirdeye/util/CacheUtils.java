/*
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

package org.apache.pinot.thirdeye.util;

import com.couchbase.client.java.document.json.JsonObject;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;
import org.apache.pinot.thirdeye.detection.cache.CacheConstants;
import org.apache.pinot.thirdeye.detection.cache.TimeSeriesDataPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility methods used for fetching and writing to the centralized cache.
 */

public class CacheUtils {

  private static final Logger LOG = LoggerFactory.getLogger(CacheUtils.class);
  // We use CRC32 as the hash function to generate keys for cache documents.
  public static CRC32 hashGenerator = new CRC32();

  /**
   * Hashes the metricURN, so that the return value can be used as a key to
   * the key-value pair in a cache document.
   * @param metricUrn metricURN string
   * @return hashed metricURN
   */
  public static String hashMetricUrn(String metricUrn) {
    hashGenerator.update(metricUrn.getBytes());
    String result = String.valueOf(hashGenerator.getValue());
    hashGenerator.reset();
    return result;
  }

  /**
   * Builds the document used to store data points in the cache.
   * Example document:
   * {
   *   "time": 123456700000
   *   "metricId": 1351714
   *   "71252492": "100.0"
   * }
   * @param point some TimeSeriesDataPoint
   * @return JsonObject with base document.
   */
  public static JsonObject buildDocumentStructure(TimeSeriesDataPoint point) {
    JsonObject body = JsonObject.create()
        .put(CacheConstants.TIMESTAMP, point.getTimestamp())
        .put(CacheConstants.METRIC_ID, point.getMetricId())
        .put(point.getMetricUrnHash(), point.getDataValueAsDouble());
    return body;
  }

  /**
   * Builds the N1QL query used to fetch data from Couchbase.
   * Example query:
   * SELECT time, `71252492` FROM `te-cache-bucket`
   *    WHERE metricId = 1351714
   *      AND `71252492` IS NOT MISSING
   *        AND time BETWEEN 100000000000 AND 200000000000
   *          ORDER BY time ASC
   * @param parameters JsonObject containing the required data to build the query.
   * @return query string
   */
  public static String buildQuery(JsonObject parameters) {
    return String.format("SELECT timestamp, `%s` FROM `%s` WHERE metricId = %d AND `%s` IS NOT MISSING AND timestamp BETWEEN %d AND %d ORDER BY time ASC",
        parameters.getString("dimensionKey"),
        parameters.getString("bucket"),
        parameters.getLong("metricId"),
        parameters.getString("dimensionKey"),
        parameters.getLong("start"),
        parameters.getLong("end"));
  }

  /**
   * Convert list of strings to their proper URL hosts. For each
   * string, it will parse out the proper host for the URI.
   * Example: "http://localhost:8091" -> "localhost"
   * @param bootstrapUris
   * @return
   */
  public static List<String> getBootstrapHosts(List<String> bootstrapUris) {
    List<String> bootstrapHosts = new ArrayList<>(bootstrapUris.size());
    for (String bootstrapUri : bootstrapUris) {
      try {
        URI uri = new URI(bootstrapUri);
        // The next workaround is to correctly parse urls that don't include any schema
        if (uri.getHost() == null) {
          uri = new URI("http://" + bootstrapUri);
        }
        String host = uri.getHost();
        bootstrapHosts.add(host);
      } catch (URISyntaxException e) {
        LOG.error("Exception while parsing host for URI {}", bootstrapUri, e);
      }
    }

    return bootstrapHosts;
  }
}
