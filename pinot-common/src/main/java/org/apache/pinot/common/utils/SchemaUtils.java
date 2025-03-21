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
import java.net.URL;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.commons.io.IOUtils;
import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.CommonConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class which contains {@link Schema} related operations.
 */
public class SchemaUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaUtils.class);

  private static final CloseableHttpClient HTTP_CLIENT = HttpClientBuilder.create().build();

  private SchemaUtils() {
  }

  /**
   * Fetch {@link Schema} from a {@link ZNRecord}.
   */
  public static Schema fromZNRecord(ZNRecord record)
      throws IOException {
    String schemaJSON = record.getSimpleField("schemaJSON");
    return Schema.fromString(schemaJSON);
  }

  /**
   * Wrap {@link Schema} into a {@link ZNRecord}.
   */
  public static ZNRecord toZNRecord(Schema schema) {
    ZNRecord record = new ZNRecord(schema.getSchemaName());
    record.setSimpleField("schemaJSON", schema.toSingleLineJsonString());
    return record;
  }

  /**
   * Given host, port and schema name, send a http GET request to download the {@link Schema}.
   *
   * @return schema on success.
   * <P><code>null</code> on failure.
   */
  @Nullable
  public static Schema getSchema(String host, int port, String schemaName) {
    Preconditions.checkNotNull(host);
    Preconditions.checkNotNull(schemaName);

    try {
      URL url = new URL(CommonConstants.HTTP_PROTOCOL, host, port, "/schemas/" + schemaName);
      HttpGet httpGet = new HttpGet(url.toString());
      try (CloseableHttpResponse response = HTTP_CLIENT.execute(httpGet)) {
        int responseCode = response.getCode();
        String responseString = EntityUtils.toString(response.getEntity());
        if (responseCode >= 400) {
          // File not find error code.
          if (responseCode == 404) {
            LOGGER.info("Cannot find schema: {} from host: {}, port: {}", schemaName, host, port);
          } else {
            LOGGER.warn("Got error response code: {}, response: {}", responseCode, response);
          }
          return null;
        }
        return Schema.fromString(responseString);
      }
    } catch (Exception e) {
      LOGGER.error("Caught exception while getting the schema: {} from host: {}, port: {}", schemaName, host, port, e);
      return null;
    }
  }

  /**
   * Given host, port and schema, send a http POST request to upload the {@link Schema}.
   *
   * @return <code>true</code> on success.
   * <P><code>false</code> on failure.
   */
  public static boolean postSchema(String host, int port, Schema schema) {
    Preconditions.checkNotNull(host);
    Preconditions.checkNotNull(schema);

    try {
      URL url = new URL(CommonConstants.HTTP_PROTOCOL, host, port, "/schemas");
      HttpPost httpPost = new HttpPost(url.toString());
      HttpEntity requestEntity =
          MultipartEntityBuilder.create().addTextBody(schema.getSchemaName(), schema.toSingleLineJsonString()).build();
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
      LOGGER.error("Caught exception while posting the schema: {} to host: {}, port: {}", schema.getSchemaName(), host,
          port, e);
      return false;
    }
  }

  /**
   * Given host, port and schema name, send a http DELETE request to delete the {@link Schema}.
   *
   * @return <code>true</code> on success.
   * <P><code>false</code> on failure.
   */
  public static boolean deleteSchema(String host, int port, String schemaName) {
    Preconditions.checkNotNull(host);
    Preconditions.checkNotNull(schemaName);

    try {
      URL url = new URL(CommonConstants.HTTP_PROTOCOL, host, port, "/schemas/" + schemaName);
      HttpDelete httpDelete = new HttpDelete(url.toString());
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
      LOGGER.error("Caught exception while getting the schema: {} from host: {}, port: {}", schemaName, host, port, e);
      return false;
    }
  }

  /**
   * Compare two schemas ignoring their version number.
   *
   * @return <code>true</code> if two schemas equal to each other.
   * <p><code>false</code>if two schemas do not equal to each other.
   */
  public static boolean equalsIgnoreVersion(Schema schema1, Schema schema2) {
    Preconditions.checkNotNull(schema1);
    Preconditions.checkNotNull(schema2);

    return schema1.getSchemaName().equals(schema2.getSchemaName()) && schema1.getFieldSpecMap()
        .equals(schema2.getFieldSpecMap());
  }

  /**
   * An example on how to use this utility class.
   */
  public static void main(String[] args) {
    Schema schema = new Schema.SchemaBuilder().setSchemaName("testSchema")
        .addSingleValueDimension("dimension", FieldSpec.DataType.DOUBLE).addMetric("metric", FieldSpec.DataType.INT)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS, "time"), null).build();
    System.out.println(postSchema("localhost", 8100, schema));
    Schema fetchedSchema = getSchema("localhost", 8100, "testSchema");
    Preconditions.checkNotNull(fetchedSchema);
    System.out.println(fetchedSchema);
    System.out.println(equalsIgnoreVersion(schema, fetchedSchema));
    System.out.println(deleteSchema("localhost", 8100, "testSchema"));
  }
}
