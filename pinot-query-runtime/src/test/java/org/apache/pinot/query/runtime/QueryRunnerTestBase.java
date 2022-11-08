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
package org.apache.pinot.query.runtime;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.query.QueryEnvironment;
import org.apache.pinot.query.QueryServerEnclosure;
import org.apache.pinot.query.QueryTestSet;
import org.apache.pinot.query.mailbox.GrpcMailboxService;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.StringUtil;
import org.testng.Assert;



public class QueryRunnerTestBase extends QueryTestSet {
  protected static final Random RANDOM_REQUEST_ID_GEN = new Random();

  protected QueryEnvironment _queryEnvironment;
  protected String _reducerHostname;
  protected int _reducerGrpcPort;
  protected Map<ServerInstance, QueryServerEnclosure> _servers = new HashMap<>();
  protected GrpcMailboxService _mailboxService;

  protected Connection _h2Connection;

  protected Connection getH2Connection() {
    Assert.assertNotNull(_h2Connection, "H2 Connection has not been initialized");
    return _h2Connection;
  }

  protected void setH2Connection()
      throws Exception {
    Assert.assertNull(_h2Connection);
    Class.forName("org.h2.Driver");
    _h2Connection = DriverManager.getConnection("jdbc:h2:mem:");
  }

  protected void addTableToH2(Schema schema, List<String> tables)
      throws SQLException {
    List<String> h2FieldNamesAndTypes = toH2FieldNamesAndTypes(schema);
    for (String tableName : tables) {
      // create table
      _h2Connection.prepareCall("DROP TABLE IF EXISTS " + tableName).execute();
      _h2Connection.prepareCall("CREATE TABLE " + tableName + " (" + StringUtil.join(",",
          h2FieldNamesAndTypes.toArray(new String[h2FieldNamesAndTypes.size()])) + ")").execute();
    }
  }

  protected void addDataToH2(Schema schema, Map<String, List<GenericRow>> rowsMap)
      throws SQLException {
    List<String> h2FieldNamesAndTypes = toH2FieldNamesAndTypes(schema);
    for (Map.Entry<String, List<GenericRow>> entry : rowsMap.entrySet()) {
      String tableName = entry.getKey();
      // remove the "_O" and "_R" suffix b/c H2 doesn't understand realtime/offline split
      if (tableName.contains("_")) {
        tableName = tableName.substring(0, tableName.length() - 2);
      }
      // insert data into table
      StringBuilder params = new StringBuilder("?");
      for (int i = 0; i < h2FieldNamesAndTypes.size() - 1; i++) {
        params.append(",?");
      }
      PreparedStatement h2Statement =
          _h2Connection.prepareStatement("INSERT INTO " + tableName + " VALUES (" + params.toString() + ")");
      for (GenericRow row : entry.getValue()) {
        int h2Index = 1;
        for (String fieldName : schema.getColumnNames()) {
          Object value = row.getValue(fieldName);
          h2Statement.setObject(h2Index++, value);
        }
        h2Statement.execute();
      }
    }
  }

  private static List<String> toH2FieldNamesAndTypes(org.apache.pinot.spi.data.Schema pinotSchema) {
    List<String> fieldNamesAndTypes = new ArrayList<>(pinotSchema.size());
    for (String fieldName : pinotSchema.getColumnNames()) {
      FieldSpec.DataType dataType = pinotSchema.getFieldSpecFor(fieldName).getDataType();
      String fieldType;
      switch (dataType) {
        case INT:
        case LONG:
          fieldType = "bigint";
          break;
        case STRING:
          fieldType = "varchar(128)";
          break;
        default:
          throw new UnsupportedOperationException("Unsupported type conversion to h2 type: " + dataType);
      }
      fieldNamesAndTypes.add(fieldName + " " + fieldType);
    }
    return fieldNamesAndTypes;
  }
}
