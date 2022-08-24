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
package org.apache.pinot.sql.parsers;

import java.util.Map;
import org.apache.calcite.sql.SqlNode;


public class SqlNodeAndOptions {
  private final SqlNode _sqlNode;
  private final PinotSqlType _sqlType;
  // TODO: support option literals other than STRING
  private final Map<String, String> _options;

  private long _parseTimeNs;

  public SqlNodeAndOptions(SqlNode sqlNode, PinotSqlType sqlType, Map<String, String> options) {
    _sqlNode = sqlNode;
    _sqlType = sqlType;
    _options = options;
  }

  public SqlNode getSqlNode() {
    return _sqlNode;
  }

  public PinotSqlType getSqlType() {
    return _sqlType;
  }

  public Map<String, String> getOptions() {
    return _options;
  }

  public long getParseTimeNs() {
    return _parseTimeNs;
  }

  public void setParseTimeNs(long parseTimeNs) {
    _parseTimeNs = parseTimeNs;
  }

  public void setExtraOptions(Map<String, String> extractOptionsMap) {
    for (Map.Entry<String, String> e : extractOptionsMap.entrySet()) {
      _options.putIfAbsent(e.getKey(), e.getValue());
    }
  }
}
