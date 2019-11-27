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
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.core.realtime.converter.RealtimeSegmentConverter;
import org.apache.pinot.core.segment.virtualcolumn.VirtualColumnProviderFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RealtimeSegmentConverterTest {

  @Test
  public void testNoVirtualColumnsInSchema() {
    Schema schema = new Schema();
    FieldSpec spec = new DimensionFieldSpec("col1", FieldSpec.DataType.STRING, true);
    schema.addField(spec);
    TimeFieldSpec tfs =
        new TimeFieldSpec("col1", FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "col2", FieldSpec.DataType.LONG,
            TimeUnit.DAYS);
    schema.addField(tfs);
    VirtualColumnProviderFactory.addBuiltInVirtualColumnsToSchema(schema);
    Assert.assertEquals(schema.getColumnNames().size(), 5);
    Assert.assertEquals(schema.getTimeFieldSpec().getIncomingGranularitySpec().getTimeType(), TimeUnit.MILLISECONDS);

    RealtimeSegmentConverter converter =
        new RealtimeSegmentConverter(null, "", schema, "testTable", "col1", "segment1", "col1");

    Schema newSchema = converter.getUpdatedSchema(schema);
    Assert.assertEquals(newSchema.getColumnNames().size(), 2);
    Assert.assertEquals(newSchema.getTimeFieldSpec().getIncomingGranularitySpec().getTimeType(), TimeUnit.DAYS);
  }

  @Test
  public void testNoTimeColumnsInSchema() {
    Schema schema = new Schema();
    schema.addField(new DimensionFieldSpec("col1", FieldSpec.DataType.STRING, true));
    schema.addField(new DimensionFieldSpec("col2", FieldSpec.DataType.STRING, true));
    schema.addField(new DimensionFieldSpec("col3", FieldSpec.DataType.STRING, true));
    schema.addField(new MetricFieldSpec("met1", FieldSpec.DataType.DOUBLE, 0));
    schema.addField(new MetricFieldSpec("met2", FieldSpec.DataType.LONG, 0));
    Assert.assertEquals(schema.getColumnNames().size(), 5);
    RealtimeSegmentConverter converter =
        new RealtimeSegmentConverter(null, "", schema, "testTable", "col1", "segment1", "col1");
    Schema newSchema = converter.getUpdatedSchema(schema);
    Assert.assertEquals(newSchema.getColumnNames().size(), 5);
  }
}
