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
package org.apache.pinot.integration.tests.window.utils;

import com.google.common.collect.ImmutableList;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


public class WindowFunnelUtils {
  public static final String DEFAULT_TABLE_NAME = "WindowFunnelTest";
  private static final String URL_COLUMN = "url";
  private static final String TIMESTAMP_COLUMN = "timestampCol";
  private static final String USER_ID_COLUMN = "userId";
  public static long _countStarResult = 0;

  private WindowFunnelUtils() {
  }

  public static Schema createSchema(String tableName) {
    return new Schema.SchemaBuilder().setSchemaName(tableName)
        .addSingleValueDimension(URL_COLUMN, FieldSpec.DataType.STRING)
        .addSingleValueDimension(TIMESTAMP_COLUMN, FieldSpec.DataType.TIMESTAMP)
        .addSingleValueDimension(USER_ID_COLUMN, FieldSpec.DataType.STRING)
        .build();
  }

  public static List<File> createAvroFiles(File tempDir)
      throws Exception {
    // create avro schema
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.createRecord("myRecord", null, null, false);
    avroSchema.setFields(ImmutableList.of(
        new org.apache.avro.Schema.Field(URL_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
            null, null),
        new org.apache.avro.Schema.Field(TIMESTAMP_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
            null, null),
        new org.apache.avro.Schema.Field(USER_ID_COLUMN,
            org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING),
            null, null)
    ));

    long[][] userTimestampValues = new long[][]{
        new long[]{1000, 1010, 1020, 1025, 1030},
        new long[]{2010, 2010, 2000},
        new long[]{1000, 1010, 1015, 1020, 11030},
        new long[]{2020, 12010, 12050},
    };
    String[][] userUrlValues = new String[][]{
        new String[]{"/product/search", "/cart/add", "/checkout/start", "/cart/add", "/checkout/confirmation"},
        new String[]{"/checkout/start", "/cart/add", "/product/search"},
        new String[]{"/product/search", "/cart/add", "/cart/add", "/checkout/start", "/checkout/confirmation"},
        new String[]{"/checkout/start", "/cart/add", "/product/search"},
    };
    int repeats = 10;
    long totalRows = 0;
    for (String[] userUrlValue : userUrlValues) {
      totalRows += userUrlValue.length;
    }
    _countStarResult = totalRows * repeats;
    // create avro file
    List<File> avroFiles = new ArrayList<>();
    for (int repeat = 0; repeat < repeats; repeat++) {
      File avroFile = new File(tempDir, "data" + repeat + ".avro");
      try (DataFileWriter<GenericData.Record> fileWriter = new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
        fileWriter.create(avroSchema, avroFile);
        for (int i = 0; i < userUrlValues.length; i++) {
          for (int j = 0; j < userUrlValues[i].length; j++) {
            GenericData.Record record = new GenericData.Record(avroSchema);
            record.put(TIMESTAMP_COLUMN, userTimestampValues[i][j]);
            record.put(URL_COLUMN, userUrlValues[i][j]);
            record.put(USER_ID_COLUMN, "user" + i + repeat);
            fileWriter.append(record);
          }
        }
      }
      avroFiles.add(avroFile);
    }
    return avroFiles;
  }
}
