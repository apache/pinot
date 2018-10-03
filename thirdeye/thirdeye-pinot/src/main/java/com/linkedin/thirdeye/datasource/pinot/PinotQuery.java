/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.datasource.pinot;

public class PinotQuery {

  private String pql;
  private String tableName;

  public PinotQuery(String pql, String tableName) {
    this.pql = pql;
    this.tableName = tableName;
  }

  public String getPql() {
    return pql;
  }

  public void setPql(String pql) {
    this.pql = pql;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  @Override
  public int hashCode() {
    return pql.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    PinotQuery that = (PinotQuery) obj;
    return this.pql.equals(that.pql);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("PinotQuery{");
    sb.append("pql='").append(pql).append('\'');
    sb.append(", tableName='").append(tableName).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
