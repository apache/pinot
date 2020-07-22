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
package org.apache.pinot.realtime.converter;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.core.realtime.converter.RealtimeSegmentConverter;
import org.apache.pinot.core.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RealtimeSegmentConverterTest {

  @Test
  public void testNoVirtualColumnsInSchema() {
    Schema schema = new Schema.SchemaBuilder().addSingleValueDimension("col1", FieldSpec.DataType.STRING)
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "col1"),
            new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.DAYS, "col2")).build();
    String segmentName = "segment1";
    VirtualColumnProviderFactory.addBuiltInVirtualColumnsToSegmentSchema(schema, segmentName);
    Assert.assertEquals(schema.getColumnNames().size(), 5);
    Schema newSchema = RealtimeSegmentConverter.getUpdatedSchema(schema);
    Assert.assertEquals(newSchema.getColumnNames().size(), 2);
  }
}
