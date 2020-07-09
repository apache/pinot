package org.apache.pinot.client.controller.response;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.ning.http.client.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


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
    private final ObjectReader OBJECT_READER = new ObjectMapper().reader();

    public TableResponseFuture(Future<Response> response, String url) {
      super(response, url);
    }

    @Override
    public TableResponse get(long timeout, TimeUnit unit)
        throws ExecutionException {
      String response = getStringResponse(timeout, unit);
      try {
        JsonNode jsonResponse = OBJECT_READER.readTree(response);
        TableResponse tableResponse = TableResponse.fromJson(jsonResponse);
        return tableResponse;
      } catch (IOException e) {
        new ExecutionException(e);
      }

      return null;
    }
  }
}
