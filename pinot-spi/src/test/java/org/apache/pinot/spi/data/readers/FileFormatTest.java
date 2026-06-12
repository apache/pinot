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
package org.apache.pinot.spi.data.readers;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class FileFormatTest {

  @DataProvider(name = "validInputFormats")
  public Object[][] validInputFormats() {
    return new Object[][]{
        {"avro", FileFormat.AVRO},
        {"AVRO", FileFormat.AVRO},
        {"Avro", FileFormat.AVRO},
        {"gzipped_avro", FileFormat.GZIPPED_AVRO},
        {"GZIPPED_AVRO", FileFormat.GZIPPED_AVRO},
        {"csv", FileFormat.CSV},
        {"CSV", FileFormat.CSV},
        {"json", FileFormat.JSON},
        {"JSON", FileFormat.JSON},
        {"orc", FileFormat.ORC},
        {"ORC", FileFormat.ORC},
        {"parquet", FileFormat.PARQUET},
        {"PARQUET", FileFormat.PARQUET},
        {"proto", FileFormat.PROTO},
        {"PROTO", FileFormat.PROTO},
        {"protobuf", FileFormat.PROTO},
        {"PROTOBUF", FileFormat.PROTO},
        {"Protobuf", FileFormat.PROTO},
        {"ProToBuF", FileFormat.PROTO},
        {"thrift", FileFormat.THRIFT},
        {"THRIFT", FileFormat.THRIFT},
        {"pinot", FileFormat.PINOT},
        {"PINOT", FileFormat.PINOT},
        {"other", FileFormat.OTHER},
        {"OTHER", FileFormat.OTHER}
    };
  }

  @Test(dataProvider = "validInputFormats")
  public void testFromString(String inputFormat, FileFormat expectedFileFormat) {
    FileFormat actualFileFormat = FileFormat.fromString(inputFormat);
    Assert.assertEquals(actualFileFormat, expectedFileFormat,
        "FileFormat mismatch for input: " + inputFormat);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testFromStringWithUnknownFormat() {
    FileFormat.fromString("unknown");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testFromStringWithEmptyString() {
    FileFormat.fromString("");
  }

  @Test
  public void testProtobufAlias() {
    // Verify that "protobuf" is correctly aliased to PROTO
    Assert.assertEquals(FileFormat.fromString("protobuf"), FileFormat.PROTO);
    Assert.assertEquals(FileFormat.fromString("PROTOBUF"), FileFormat.PROTO);
    Assert.assertEquals(FileFormat.fromString("Protobuf"), FileFormat.PROTO);

    // Verify that "proto" also works
    Assert.assertEquals(FileFormat.fromString("proto"), FileFormat.PROTO);
    Assert.assertEquals(FileFormat.fromString("PROTO"), FileFormat.PROTO);
  }
}
