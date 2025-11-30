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
package org.apache.pinot.client.admin;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * The <code>SegmentApiClient</code> class provides a http client for invoking segment rest apis.
 */
public class PinotSegmentApiClient implements Closeable {
  private final PinotAdminTransport _transport;
  private final String _controllerAddress;
  private final Map<String, String> _headers;

  public PinotSegmentApiClient(PinotAdminTransport transport, String controllerAddress,
      Map<String, String> headers) {
    _transport = transport;
    _controllerAddress = controllerAddress;
    _headers = headers;
  }

  public static class QueryParameters {
    public static final String TABLE_NAME = "tableName";
    public static final String TYPE = "type";
    public static final String START_TIMESTAMP = "startTimestamp";
    public static final String END_TIMESTAMP = "endTimestamp";
    public static final String EXCLUDE_OVERLAPPING = "excludeOverlapping";
  }

  private static final String SEGMENT_PATH = "/segments";
  private static final String SELECT_PATH = "/select";
  private static final String METADATA_PATH = "/metadata";

  public JsonNode selectSegments(String rawTableName, String tableType, long startTimestamp, long endTimestamp,
      boolean excludeOverlapping) throws PinotAdminException {
    Map<String, String> queryParams = new HashMap<>();
    queryParams.put(QueryParameters.START_TIMESTAMP, String.valueOf(startTimestamp));
    queryParams.put(QueryParameters.END_TIMESTAMP, String.valueOf(endTimestamp));
    queryParams.put(QueryParameters.EXCLUDE_OVERLAPPING, String.valueOf(excludeOverlapping));
    queryParams.put(QueryParameters.TYPE, tableType);
    return _transport.executeGet(_controllerAddress,
        SEGMENT_PATH + "/" + PinotAdminPathUtils.encodePathSegment(rawTableName) + SELECT_PATH, queryParams, _headers);
  }

  public JsonNode getSegmentMetadata(String tableNameWithType, String segmentName) throws PinotAdminException {
    return _transport.executeGet(_controllerAddress,
        SEGMENT_PATH + "/" + PinotAdminPathUtils.encodePathSegment(tableNameWithType) + "/"
            + PinotAdminPathUtils.encodePathSegment(segmentName) + METADATA_PATH,
        null, _headers);
  }

  @Override
  public void close()
      throws IOException {
    _transport.close();
  }
}
