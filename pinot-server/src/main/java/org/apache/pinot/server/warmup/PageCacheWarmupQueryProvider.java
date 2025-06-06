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
package org.apache.pinot.server.warmup;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.HttpVersion;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.http.HttpClient;
import org.apache.pinot.common.utils.http.HttpClientConfig;
import org.apache.pinot.common.utils.tls.TlsUtils;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.RetryPolicies;
import org.apache.pinot.spi.utils.retry.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class PageCacheWarmupQueryProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(PageCacheWarmupQueryProvider.class);

  private final URI _pinotControllerUri;
  private final HttpClient _httpClient;

  private static final String TABLE_PATH_PREFIX = "/linkedin/pagecache/queries";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public PageCacheWarmupQueryProvider(URI pinotControllerUri) {
    _pinotControllerUri = pinotControllerUri;
    _httpClient = new HttpClient(HttpClientConfig.DEFAULT_HTTP_CLIENT_CONFIG, TlsUtils.getSslContext());
  }

  public List<String> getWarmupQueries(String tableNameWithType) {
    try {
      Pair<String, String> tableInfo = extractTableInfo(tableNameWithType);
      if (tableInfo == null) {
        LOGGER.error("Failed to extract table info from table name with type: {}", tableNameWithType);
        return null;
      }

      // Construct request
      URI warmupQueriesUri = getWarmupQueriesURI(tableInfo.getLeft(), tableInfo.getRight());
      ClassicHttpRequest request = ClassicRequestBuilder.get(warmupQueriesUri)
          .setVersion(HttpVersion.HTTP_1_1)
          .setHeader(HttpHeaders.CONTENT_TYPE, HttpClient.JSON_CONTENT_TYPE)
          .build();
      LOGGER.info("Sending request: {} to get queries from deep store for table: {}", request, tableNameWithType);

      // Retry logic with AtomicReference
      AtomicReference<List<String>> queries = new AtomicReference<>(null);
      RetryPolicy retryPolicy = RetryPolicies.exponentialBackoffRetryPolicy(3, 3000L, 1.2f);
      retryPolicy.attempt(() -> {
        try {
          SimpleHttpResponse response = HttpClient.wrapAndThrowHttpException(
              _httpClient.sendRequest(request, HttpClient.DEFAULT_SOCKET_TIMEOUT_MS)
          );
          if (response.getStatusCode() == HttpStatus.SC_OK) {
            String responseBody = response.getResponse();
            List<String> resultQueries = OBJECT_MAPPER.readValue(responseBody, new TypeReference<List<String>>() { });
            queries.set(resultQueries); // Set the value inside AtomicReference
            LOGGER.info("Successfully got {} queries from deep store for table: {}, queries: {}",
                resultQueries.size(), tableNameWithType, resultQueries);
            return true;
          } else {
            LOGGER.error("Failed to get queries from deep store for table: {}, response: {}",
                tableNameWithType, response);
            return false;
          }
        } catch (Exception e) {
          LOGGER.error("Error sending request to server for table: {}", tableNameWithType, e);
          return false;
        }
      });
      return queries.get();
    } catch (Exception e) {
      LOGGER.error("Error when getting queries from deep store for table: {}", tableNameWithType, e);
      return null;
    }
  }

  public static Pair<String, String> extractTableInfo(String tableNameWithType) {
    if (tableNameWithType == null) {
      return null;
    }
    String tableName = TableNameBuilder.extractRawTableName(tableNameWithType);
    TableType tableType = TableNameBuilder.getTableTypeFromTableName(tableNameWithType);
    if (tableType == null) {
      return null;
    }
    return new ImmutablePair<>(tableName, tableType.toString());
  }

  private URI getWarmupQueriesURI(String tableName, String tableType) throws URISyntaxException {
    return new URI(_pinotControllerUri + TABLE_PATH_PREFIX + "/" + tableName + "?tableType=" + tableType);
  }
}
