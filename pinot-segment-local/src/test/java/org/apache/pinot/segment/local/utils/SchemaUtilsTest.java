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
package org.apache.pinot.segment.local.utils;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;


@SuppressWarnings("deprecation")
public class SchemaUtilsTest {
  @Test
  public void testValidateRejectsBucketedEpochIncomingTimeFieldSpec() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testSchema")
        .addSingleValueDimension("dimension", DataType.STRING)
        .addTime(new TimeGranularitySpec(DataType.LONG, 5, TimeUnit.MINUTES, "eventTime"), null)
        .build();

    IllegalStateException exception = Assert.expectThrows(IllegalStateException.class,
        () -> SchemaUtils.validate(schema));

    assertThat(exception).hasMessageContaining("only supports timeUnitSize 1")
        .hasMessageContaining("Use DateTimeFieldSpec");
  }

  @Test
  public void testValidateRejectsBucketedEpochOutgoingTimeFieldSpec() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testSchema")
        .addSingleValueDimension("dimension", DataType.STRING)
        .addTime(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "incomingTime"),
            new TimeGranularitySpec(DataType.LONG, 5, TimeUnit.MINUTES, "eventTime"))
        .build();

    IllegalStateException exception = Assert.expectThrows(IllegalStateException.class,
        () -> SchemaUtils.validate(schema));

    assertThat(exception).hasMessageContaining("only supports timeUnitSize 1")
        .hasMessageContaining("Use DateTimeFieldSpec");
  }

  @Test
  public void testValidateAcceptsEpochTimeUnitSizeOneTimeFieldSpec() {
    Schema schema = new Schema.SchemaBuilder()
        .setSchemaName("testSchema")
        .addSingleValueDimension("dimension", DataType.STRING)
        .addTime(new TimeGranularitySpec(DataType.LONG, TimeUnit.MILLISECONDS, "eventTime"), null)
        .build();

    SchemaUtils.validate(schema);
  }
}
