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
package org.apache.pinot.spi.ingestion.batch.spec;

import java.io.Serializable;


/**
 * TableSpec defines table name and where to fetch corresponding table config and table schema.
 */
public class TableSpec implements Serializable {

  /**
   * Table name
   */
  String _tableName;

  /**
   * schemaURI defines where to read the table schema.
   * Supports using PinotFS or HTTP.
   * E.g. hdfs://path/to/table_schema.json
   *      http://localhost:9000/tables/myTable/schema
   */
  String _schemaURI;

  /**
   * tableConfigURI defines where to reade the table config.
   * Supports using PinotFS or HTTP.
   * E.g. hdfs://path/to/table_config.json
   *      http://localhost:9000/tables/myTable
   * Note that the API to read Pinot table config directly from pinot controller contains a JSON wrapper.
   * The real table config is the object under the field 'OFFLINE'.
   */
  String _tableConfigURI;

  public String getTableName() {
    return _tableName;
  }

  public void setTableName(String tableName) {
    _tableName = tableName;
  }

  public String getSchemaURI() {
    return _schemaURI;
  }

  public void setSchemaURI(String schemaURI) {
    _schemaURI = schemaURI;
  }

  public String getTableConfigURI() {
    return _tableConfigURI;
  }

  public void setTableConfigURI(String tableConfigURI) {
    _tableConfigURI = tableConfigURI;
  }
}
