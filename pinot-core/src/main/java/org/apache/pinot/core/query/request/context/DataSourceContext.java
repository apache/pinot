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
package org.apache.pinot.core.query.request.context;

public class DataSourceContext {
  private final String _tableName;
  private final QueryContext _subquery;
  private final JoinContext _join;

  public DataSourceContext(String tableName, QueryContext subquery, JoinContext join) {
    _tableName = tableName;
    _subquery = subquery;
    _join = join;
  }

  public String getTableName() {
    return _tableName;
  }

  public QueryContext getSubquery() {
    return _subquery;
  }

  public JoinContext getJoin() {
    return _join;
  }

  @Override
  public String toString() {
    return "DataSourceContext{" + "_tableName='" + _tableName + '\'' + ", _subquery=" + _subquery + ", _join=" + _join
        + '}';
  }
}
