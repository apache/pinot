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
package org.apache.pinot.integration.tests.tpch.generator;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.client.ResultSet;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;


public class PinotQueryBasedColumnDataProvider implements SampleColumnDataProvider {
  public interface PinotConnectionProvider {
    org.apache.pinot.client.Connection getConnection();
  }

  private static final String COUNT_START_QUERY_FORMAT = "SELECT COUNT(*) FROM %s";
  private static final String QUERY_FORMAT = "SELECT %s FROM %s WHERE $docId IN (%s)";
  private final PinotConnectionProvider _pinotConnectionProvider;

  public PinotQueryBasedColumnDataProvider(PinotConnectionProvider connectionProvider) {
    _pinotConnectionProvider = connectionProvider;
  }

  @Override
  public Pair<Boolean, List<String>> getSampleValues(String tableName, String columnName)
      throws JSONException {
    String countStarQuery = String.format(COUNT_START_QUERY_FORMAT, tableName);
    boolean isMultiValue = false;
    int count = _pinotConnectionProvider.getConnection().execute(countStarQuery).getResultSet(0).getInt(0);

    StringBuilder randomDocIds = new StringBuilder();

    for (int i = 0; i < 10; i++) {
      randomDocIds.append((int) (Math.random() * count));
      if (i != 9) {
        randomDocIds.append(", ");
      }
    }

    String query = String.format(QUERY_FORMAT, columnName, tableName, randomDocIds);

    List<String> columnValues = new ArrayList<>();
    ResultSet resultSet = _pinotConnectionProvider.getConnection().execute(query).getResultSet(0);

    for (int i = 0; i < resultSet.getRowCount(); i++) {
      if (resultSet.getColumnDataType(0).contains("ARRAY")) {
        String array = resultSet.getString(i, 0);
        JSONArray jsnobject = new JSONArray(array);
        columnValues.add(jsnobject.get(0).toString());
        isMultiValue = true;
      } else {
        columnValues.add(resultSet.getString(i, 0));
      }
    }

    return Pair.of(isMultiValue, columnValues);
  }
}
