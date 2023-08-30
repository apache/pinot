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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.utils.JsonUtils;
import org.asynchttpclient.Response;


public class TableResponse {
  private JsonNode _tables;

  private TableResponse() {
  }

  private TableResponse(JsonNode tableResponse) {
    _tables = tableResponse.get("tables");
  }

  public static TableResponse fromJson(JsonNode tableResponse) {
    return new TableResponse(tableResponse);
  }

  public static TableResponse empty() {
    return new TableResponse();
  }

  public List<String> getAllTables() {
    List<String> allTables = new ArrayList<>();
    if (_tables == null) {
      return allTables;
    }

    if (_tables.isArray()) {
      for (JsonNode table : _tables) {
        allTables.add(table.textValue());
      }
    }

    return allTables;
  }

  public int getNumTables() {
    if (_tables == null) {
      return 0;
    } else {
      return _tables.size();
    }
  }

  public static class TableResponseFuture extends ControllerResponseFuture<TableResponse> {
    public TableResponseFuture(Future<Response> response, String url) {
      super(response, url);
    }

    @Override
    public TableResponse get(long timeout, TimeUnit unit)
        throws ExecutionException {
      try {
        InputStream response = getStreamResponse(timeout, unit);
        JsonNode jsonResponse = JsonUtils.inputStreamToJsonNode(response);
        return TableResponse.fromJson(jsonResponse);
      } catch (IOException e) {
        throw new ExecutionException(e);
      }
    }
  }
}
