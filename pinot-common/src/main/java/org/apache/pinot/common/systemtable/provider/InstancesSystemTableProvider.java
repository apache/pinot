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
package org.apache.pinot.common.systemtable.provider;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.response.broker.BrokerResponseNative;
import org.apache.pinot.common.systemtable.SystemTableDataProvider;
import org.apache.pinot.common.systemtable.SystemTableResponseUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.systemtable.SystemTable;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;


/**
 * Basic system table exposing Pinot instance metadata.
 */
@SystemTable
public final class InstancesSystemTableProvider implements SystemTableDataProvider {
  public static final String TABLE_NAME = "system.instances";

  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName(TABLE_NAME)
      .addSingleValueDimension("instanceId", FieldSpec.DataType.STRING)
      .addSingleValueDimension("type", FieldSpec.DataType.STRING)
      .addSingleValueDimension("host", FieldSpec.DataType.STRING)
      .addSingleValueDimension("port", FieldSpec.DataType.INT)
      .addSingleValueDimension("state", FieldSpec.DataType.STRING)
      .addSingleValueDimension("tags", FieldSpec.DataType.STRING)
      .build();

  @Override
  public String getTableName() {
    return TABLE_NAME;
  }

  @Override
  public Schema getSchema() {
    return SCHEMA;
  }

  @Override
  public TableConfig getTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE).setTableName(TABLE_NAME).build();
  }

  @Override
  public BrokerResponseNative getBrokerResponse(PinotQuery pinotQuery) {
    // Controller/Helix fetch can be added later; for now return an empty result.
    List<String> projectionColumns = pinotQuery.getSelectList() != null
        ? pinotQuery.getSelectList().stream().map(expr -> expr.getIdentifier().getName()).collect(Collectors.toList())
        : List.of();
    return SystemTableResponseUtils.buildBrokerResponse(
        TABLE_NAME, SCHEMA, projectionColumns, Collections.emptyList(), 0);
  }
}
