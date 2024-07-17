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
package org.apache.pinot.integration.tests.custom;

import com.beust.jcommander.internal.Lists;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.util.TimeZone;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.common.function.scalar.DateTimeFunctions;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TimestampConfig;
import org.apache.pinot.spi.config.table.TimestampIndexGranularity;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.avro.Schema.create;
import static org.testng.Assert.assertEquals;


@Test(suiteName = "CustomClusterIntegrationTest")
public class TimestampIndexTest extends CustomDataQueryClusterIntegrationTest {

  private static final String DEFAULT_TABLE_NAME = "TimestampTest";
  private static final String TIMESTAMP_BASE = "tsBase";

  private static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getDefault();

  @Override
  protected long getCountStarResult() {
    return 1000;
  }

  @BeforeClass
  public void setUpTimeZone() {
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
  }

  @AfterClass
  public void removeTimeZone() {
    TimeZone.setDefault(DEFAULT_TIME_ZONE);
  }

  @Test(dataProvider = "useBothQueryEngines")
  public void testFirstWithTimeQueries(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String query = String.format(
        "SELECT tsBase FROM %s\n",
        getTableName());
    JsonNode jsonNode = postQuery(query);
    assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(0).longValue(), 1546300800000L);
    assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(1).longValue(), 1546300800000L);
    assertEquals(jsonNode.get("resultTable").get("rows").get(0).get(2).longValue(), 1546300800000L);
  }

  @Override
  public String getTableName() {
    return DEFAULT_TABLE_NAME;
  }

  @Override
  public Schema createSchema() {
    return new Schema.SchemaBuilder().setSchemaName(getTableName())
        .addSingleValueDimension(TIMESTAMP_BASE, FieldSpec.DataType.TIMESTAMP)
        .build();
  }

  @Override
  public TableConfig createOfflineTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(getTableName())
        .setFieldConfigList(Lists.newArrayList(
            new FieldConfig.Builder(TIMESTAMP_BASE)
                .withEncodingType(FieldConfig.EncodingType.DICTIONARY)
                .withTimestampConfig(
                    new TimestampConfig(Lists.newArrayList(
                        TimestampIndexGranularity.DAY,
                        TimestampIndexGranularity.HOUR
                    ))
                )
                .build()
        ))
        .build();
  }

  @Override
  public File createAvroFile()
      throws Exception {
    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
        new Field(TIMESTAMP_BASE, create(Type.LONG), null, null)
    ));

    // create avro file
    File avroFile = new File(_tempDir, "data.avro");
    try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      fileWriter.create(avroSchema, avroFile);
      long tsBaseLong = DateTimeFunctions.fromDateTime("2019-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss");
      for (int i = 0; i < getCountStarResult(); i++) {
        // create avro record
        GenericData.Record record = new GenericData.Record(avroSchema);
        record.put(TIMESTAMP_BASE, tsBaseLong);

        // add avro record to file
        fileWriter.append(record);
        tsBaseLong += 86400000;
      }
    }
    return avroFile;
  }
}
