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
package org.apache.pinot.client.controller.response;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.utils.JsonUtils;
import org.asynchttpclient.Response;


public class SchemaResponse {
  private String _schemaName;
  private JsonNode _dimensions;
  private JsonNode _metrics;
  private JsonNode _dateTimeFieldSpecs;

  private SchemaResponse() {
  }

  private SchemaResponse(JsonNode schemaResponse) {
    _schemaName = schemaResponse.get("schemaName").textValue();
    _dimensions = schemaResponse.get("dimensionFieldSpecs");
    _metrics = schemaResponse.get("metricFieldSpecs");
    _dateTimeFieldSpecs = schemaResponse.get("dateTimeFieldSpecs");
  }

  public static SchemaResponse fromJson(JsonNode schemaResponse) {
    return new SchemaResponse(schemaResponse);
  }

  public static SchemaResponse empty() {
    return new SchemaResponse();
  }

  public String getSchemaName() {
    return _schemaName;
  }

  public JsonNode getDimensions() {
    return _dimensions;
  }

  public JsonNode getMetrics() {
    return _metrics;
  }

  public JsonNode getDateTimeFieldSpecs() {
    return _dateTimeFieldSpecs;
  }

  public static class SchemaResponseFuture extends ControllerResponseFuture<SchemaResponse> {
    public SchemaResponseFuture(Future<Response> response, String url) {
      super(response, url);
    }

    @Override
    public SchemaResponse get(long timeout, TimeUnit unit)
        throws ExecutionException {
      try {
        InputStream response = getStreamResponse(timeout, unit);
        JsonNode jsonResponse = JsonUtils.inputStreamToJsonNode(response);
        return SchemaResponse.fromJson(jsonResponse);
      } catch (IOException e) {
        throw new ExecutionException(e);
      }
    }
  }
}
