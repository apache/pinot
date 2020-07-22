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
package org.apache.pinot.core.data.recordtransformer;

import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.testng.Assert.fail;


/**
 * Tests to check transformation of time column using time field spec
 */
public class ExpressionTransformerTimeTest {

  @Test
  public void testTimeSpecConversion() {
    long validMillis = 1585724400000L;
    long validHours = 440479;
    ExpressionTransformer expressionTransformer;

    // all combinations of timespec, and values in the incoming outgoing
    Schema pinotSchema;
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTimeSpecTransformer").build();

    // 1] only incoming defined - doesn't create DefaultTimeSpecEvaluator. Incoming value used as is.
    pinotSchema = new Schema.SchemaBuilder()
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "incoming"), null).build();

    // correct value for incoming - use incoming
    expressionTransformer = new ExpressionTransformer(tableConfig, pinotSchema);
    GenericRow genericRow = new GenericRow();
    genericRow.putValue("incoming", validMillis);
    expressionTransformer.transform(genericRow);
    Assert.assertEquals(genericRow.getValue("incoming"), validMillis);

    // incorrect value - use whatever is
    expressionTransformer = new ExpressionTransformer(tableConfig, pinotSchema);
    genericRow = new GenericRow();
    genericRow.putValue("incoming", -1);
    expressionTransformer.transform(genericRow);
    Assert.assertEquals(genericRow.getValue("incoming"), -1);

    // 2] both incoming and outgoing defined - exactly the same - Doesn't create DefaultTimeFSpecEvaluator, incoming used as is.
    pinotSchema = new Schema.SchemaBuilder()
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "time"),
            new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "time")).build();

    // correct value - use incoming
    expressionTransformer = new ExpressionTransformer(tableConfig, pinotSchema);
    genericRow = new GenericRow();
    genericRow.putValue("time", validMillis);
    expressionTransformer.transform(genericRow);
    Assert.assertEquals(genericRow.getValue("time"), validMillis);

    // incorrect value - use whatever is
    expressionTransformer = new ExpressionTransformer(tableConfig, pinotSchema);
    genericRow = new GenericRow();
    genericRow.putValue("time", -1);
    expressionTransformer.transform(genericRow);
    Assert.assertEquals(genericRow.getValue("time"), -1);

    // 3] both incoming and outgoing defined - same column name only - skip conversion - this shouldn't be allowed by validation during add schema
    try {
      pinotSchema = new Schema.SchemaBuilder()
          .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "time"),
              new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.HOURS, "time")).build();
      new ExpressionTransformer(tableConfig, pinotSchema);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }

    // 4] both incoming and outgoing defined - different column names - convert when possible
    pinotSchema = new Schema.SchemaBuilder()
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, "incoming"),
            new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.HOURS, "outgoing")).build();

    // only valid incoming value exists - convert using DefaultTimeSpecEvaluator
    expressionTransformer = new ExpressionTransformer(tableConfig, pinotSchema);
    genericRow = new GenericRow();
    genericRow.putValue("incoming", validMillis);
    expressionTransformer.transform(genericRow);
    Assert.assertEquals(genericRow.getValue("outgoing"), validHours);

    // only valid outgoing value exists - Never invokes transformation using DefaultTimeSpecEvaluator, as outgoing already present
    expressionTransformer = new ExpressionTransformer(tableConfig, pinotSchema);
    genericRow = new GenericRow();
    genericRow.putValue("outgoing", validHours);
    expressionTransformer.transform(genericRow);
    Assert.assertEquals(genericRow.getValue("outgoing"), validHours);

    // only invalid incoming value exists - exception when validating
    expressionTransformer = new ExpressionTransformer(tableConfig, pinotSchema);
    genericRow = new GenericRow();
    genericRow.putValue("incoming", -1);
    try {
      expressionTransformer.transform(genericRow);
      Assert.fail();
    } catch (Exception e) {
      // expected
    }

    // only invalid outgoing value exists - Never invokes transformation using DefaultTimeSpecEvaluator, as outgoing already present
    expressionTransformer = new ExpressionTransformer(tableConfig, pinotSchema);
    genericRow = new GenericRow();
    genericRow.putValue("outgoing", -1);
    expressionTransformer.transform(genericRow);
    Assert.assertEquals(genericRow.getValue("outgoing"), -1);

    // both valid incoming and outgoing exist - Never invokes transformation using DefaultTimeSpecEvaluator, as outgoing already present
    expressionTransformer = new ExpressionTransformer(tableConfig, pinotSchema);
    genericRow = new GenericRow();
    genericRow.putValue("incoming", validMillis);
    genericRow.putValue("outgoing", validHours);
    expressionTransformer.transform(genericRow);
    Assert.assertEquals(genericRow.getValue("outgoing"), validHours);

    // invalid incoming, valid outgoing - Never invokes transformation using DefaultTimeSpecEvaluator, as outgoing already present
    expressionTransformer = new ExpressionTransformer(tableConfig, pinotSchema);
    genericRow = new GenericRow();
    genericRow.putValue("incoming", -1);
    genericRow.putValue("outgoing", validHours);
    expressionTransformer.transform(genericRow);
    Assert.assertEquals(genericRow.getValue("outgoing"), validHours);

    // valid incoming, invalid outgoing - Never invokes transformation using DefaultTimeSpecEvaluator, as outgoing already present
    expressionTransformer = new ExpressionTransformer(tableConfig, pinotSchema);
    genericRow = new GenericRow();
    genericRow.putValue("incoming", validMillis);
    genericRow.putValue("outgoing", -1);
    expressionTransformer.transform(genericRow);
    Assert.assertEquals(genericRow.getValue("outgoing"), -1);

    // invalid incoming and outgoing - Never invokes transformation using DefaultTimeSpecEvaluator, as outgoing already present
    expressionTransformer = new ExpressionTransformer(tableConfig, pinotSchema);
    genericRow = new GenericRow();
    genericRow.putValue("incoming", -1);
    genericRow.putValue("outgoing", -1);
    expressionTransformer.transform(genericRow);
    Assert.assertEquals(genericRow.getValue("outgoing"), -1);

    // 5] SIMPLE_DATE_FORMAT
    // When incoming and outgoing spec are the same, any time format should work
    pinotSchema = new Schema.SchemaBuilder().addTime(new TimeGranularitySpec(FieldSpec.DataType.INT, TimeUnit.DAYS,
        TimeGranularitySpec.TimeFormat.SIMPLE_DATE_FORMAT.toString(), "time"), null).build();
    expressionTransformer = new ExpressionTransformer(tableConfig, pinotSchema);
    genericRow = new GenericRow();
    genericRow.putValue("time", 20180101);
    expressionTransformer.transform(genericRow);
    Assert.assertEquals(genericRow.getValue("time"), 20180101);
  }
}
