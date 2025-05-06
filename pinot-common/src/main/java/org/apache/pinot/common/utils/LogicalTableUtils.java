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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.io.IOUtils;
import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.HttpEntities;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.data.LogicalTable;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class which contains {@link LogicalTable} related operations.
 */
public class LogicalTableUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(LogicalTableUtils.class);

  private static final CloseableHttpClient HTTP_CLIENT = HttpClients.createDefault();

  private LogicalTableUtils() {
  }

  /**
   * Fetch {@link LogicalTable} from a {@link ZNRecord}.
   */
  public static LogicalTable fromZNRecord(@Nonnull ZNRecord record)
      throws IOException {
    String tableJSON = record.getSimpleField("tableJSON");
    return LogicalTable.fromString(tableJSON);
  }

  /**
   * Wrap {@link LogicalTable} into a {@link ZNRecord}.
   */
  public static ZNRecord toZNRecord(@Nonnull LogicalTable table) {
    ZNRecord record = new ZNRecord(table.getTableName());
    record.setSimpleField("tableJSON", table.toSingleLineJsonString());
    return record;
  }

  /**
   * Given host, port and table name, send a http GET request to download the {@link LogicalTable}.
   *
   * @return table on success.
   * <P><code>null</code> on failure.
   */
  public static @Nullable LogicalTable getTable(@Nonnull String host, int port, @Nonnull String tableName) {
    Preconditions.checkNotNull(host);
    Preconditions.checkNotNull(tableName);

    try {
      URI uri = new URI(CommonConstants.HTTP_PROTOCOL, null, host, port, "/logicalTables/" + tableName, null, null);
      HttpGet httpGet = new HttpGet(uri);
      try (CloseableHttpResponse response = HTTP_CLIENT.execute(httpGet)) {
        int responseCode = response.getCode();
        String responseString = EntityUtils.toString(response.getEntity());
        if (responseCode >= 400) {
          // File not find error code.
          if (responseCode == 404) {
            LOGGER.info("Cannot find table: {} from host: {}, port: {}", tableName, host, port);
          } else {
            LOGGER.warn("Got error response code: {}, response: {}", responseCode, response);
          }
          return null;
        }
        return LogicalTable.fromString(responseString);
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while getting the table: {} from host: {}, port: {}", tableName, host, port, e);
      return null;
    }
  }

  /**
   * Given host, port and table, send a http POST request to upload the {@link LogicalTable}.
   *
   * @return <code>true</code> on success.
   * <P><code>false</code> on failure.
   */
  public static boolean postTable(@Nonnull String host, int port, @Nonnull LogicalTable table) {
    Preconditions.checkNotNull(host);
    Preconditions.checkNotNull(table);

    try {
      URI uri = new URI(CommonConstants.HTTP_PROTOCOL, null, host, port, "/logicalTables", null, null);
      HttpPost httpPost = new HttpPost(uri);
      HttpEntity requestEntity = HttpEntities.create(table.toSingleLineJsonString(), ContentType.APPLICATION_JSON);
      httpPost.setEntity(requestEntity);
      try (CloseableHttpResponse response = HTTP_CLIENT.execute(httpPost)) {
        int responseCode = response.getCode();
        if (responseCode >= 400) {
          String responseString = IOUtils.toString(response.getEntity().getContent(), Charset.defaultCharset());
          LOGGER.warn("Got error response code: {}, response: {}", responseCode, responseString);
          return false;
        }
        return true;
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while posting the table: {} to host: {}, port: {}", table.getTableName(), host,
          port, e);
      return false;
    }
  }

  /**
   * Given host, port and table name, send a http DELETE request to delete the {@link LogicalTable}.
   *
   * @return <code>true</code> on success.
   * <P><code>false</code> on failure.
   */
  public static boolean deleteTable(@Nonnull String host, int port, @Nonnull String tableName) {
    Preconditions.checkNotNull(host);
    Preconditions.checkNotNull(tableName);

    try {
      URI uri = new URI(CommonConstants.HTTP_PROTOCOL, null, host, port, "/logicalTables/" + tableName, null, null);
      HttpDelete httpDelete = new HttpDelete(uri);
      try (CloseableHttpResponse response = HTTP_CLIENT.execute(httpDelete)) {
        int responseCode = response.getCode();
        if (responseCode >= 400) {
          String responseString = EntityUtils.toString(response.getEntity());
          LOGGER.warn("Got error response code: {}, response: {}", responseCode, responseString);
          return false;
        }
        return true;
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while getting the table: {} from host: {}, port: {}", tableName, host, port, e);
      return false;
    }
  }
}
