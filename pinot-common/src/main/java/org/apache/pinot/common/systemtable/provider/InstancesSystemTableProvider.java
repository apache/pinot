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
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.systemtable.SystemTableProvider;
import org.apache.pinot.spi.systemtable.SystemTableRequest;
import org.apache.pinot.spi.systemtable.SystemTableResponse;


/**
 * Basic system table exposing Pinot instance metadata.
 */
public final class InstancesSystemTableProvider implements SystemTableProvider {
  private static final Schema SCHEMA = new Schema.SchemaBuilder().setSchemaName("system.instances")
      .addSingleValueDimension("instanceId", FieldSpec.DataType.STRING)
      .addSingleValueDimension("type", FieldSpec.DataType.STRING)
      .addSingleValueDimension("host", FieldSpec.DataType.STRING)
      .addSingleValueDimension("port", FieldSpec.DataType.INT)
      .addSingleValueDimension("state", FieldSpec.DataType.STRING)
      .addSingleValueDimension("tags", FieldSpec.DataType.STRING)
      .build();

  @Override
  public String getTableName() {
    return "system.instances";
  }

  @Override
  public Schema getSchema() {
    return SCHEMA;
  }

  @Override
  public SystemTableResponse getRows(SystemTableRequest request) {
    // Controller/Helix fetch can be added later.
    return new SystemTableResponse(Collections.emptyList(), System.currentTimeMillis(), 0);
  }
}
