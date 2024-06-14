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
package org.apache.pinot.query.catalog;

import com.beust.jcommander.internal.Lists;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.pinot.query.type.TypeFactory;
import org.apache.pinot.query.type.TypeSystem;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TimestampConfig;
import org.apache.pinot.spi.config.table.TimestampIndexGranularity;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PinotTableTest {

  @Test
  public void testTimestampFields() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("myTable")
        .addSingleValueDimension("ts", FieldSpec.DataType.TIMESTAMP)
        .build();
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE)
        .setTableName("myTable")
        .setFieldConfigList(Lists.newArrayList(
            new FieldConfig.Builder("ts")
                .withTimestampConfig(
                    new TimestampConfig(Lists.newArrayList(
                        TimestampIndexGranularity.DAY,
                        TimestampIndexGranularity.HOUR
                    ))
                )
                .build()
        ))
        .build();

    PinotTable pinotTable = new PinotTable(schema, tableConfig);
    TypeFactory relDataTypeFactory = new TypeFactory(new TypeSystem());
    RelDataType rowType = pinotTable.getRowType(relDataTypeFactory);

    List<String> actualFieldNames = rowType.getFieldList().stream()
        .map(RelDataTypeField::getName)
        .map(name -> name.toLowerCase(Locale.US))
        .collect(Collectors.toList());
    List<String> expectedFieldNames = Lists.newArrayList("$ts$day", "$ts$hour", "ts");

    Assert.assertEquals(actualFieldNames, expectedFieldNames, "Unexpected field list");
  }
}
