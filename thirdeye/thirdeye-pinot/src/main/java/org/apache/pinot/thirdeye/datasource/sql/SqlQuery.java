/*
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

package org.apache.pinot.thirdeye.datasource.sql;

import java.util.List;
import org.apache.pinot.thirdeye.common.time.TimeGranularity;
import org.apache.pinot.thirdeye.common.time.TimeSpec;
import org.apache.pinot.thirdeye.datasource.RelationalQuery;


public class SqlQuery extends RelationalQuery {

  private String sourceName;
  private String dbName;
  private String metric;
  private List<String> groupByKeys;
  private TimeGranularity granularity;
  private TimeSpec timeSpec;


  public SqlQuery(String sql, String sourceName, String dbName, String metric, List<String> groupByKeys, TimeGranularity granularity, TimeSpec timeSpec) {
    super(sql);
    this.sourceName = sourceName;
    this.dbName = dbName;
    this.metric = metric;
    this.groupByKeys = groupByKeys;
    this.granularity = granularity;
    this.timeSpec = timeSpec;
  }

  public String getQuery() {
    return query;
  }

  public void setSql(String sql) {
    this.query = query;
  }

  public String getSourceName() {
    return sourceName;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public List<String> getGroupByKeys() {
    return groupByKeys;
  }

  public void setGroupByKeys(List<String> groupByKeys) {
    this.groupByKeys = groupByKeys;
  }

  public TimeGranularity getGranularity() {
    return granularity;
  }

  public void setGranularity(TimeGranularity granularity) {
    this.granularity = granularity;
  }

  public TimeSpec getTimeSpec() {
    return timeSpec;
  }

  public void setTimeSpec(TimeSpec timeSpec) {
    this.timeSpec = timeSpec;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("SqlQuery{");
    sb.append("sql='").append(query).append('\'');
    sb.append(", sourceName=").append(sourceName).append('\'');
    sb.append(", dbName=").append(dbName).append('\'');
    sb.append(", metric=").append(metric).append('\'');
    sb.append(", groupByKeys=").append(groupByKeys).append('\'');
    sb.append(", granularity=").append(granularity).append('\'');
    sb.append(", timeSpec=").append(timeSpec).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
