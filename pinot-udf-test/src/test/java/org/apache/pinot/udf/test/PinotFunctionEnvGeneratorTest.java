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
package org.apache.pinot.udf.test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;


/// Tests schema generation for logical UUID UDF parameters and results.
public class PinotFunctionEnvGeneratorTest {
  @Test
  public void testGenerateSchemaWithUuidColumns() {
    Schema schema = PinotFunctionEnvGenerator.generateSchema(List.of(new TestUuidUdf()));

    FieldSpec input = schema.getFieldSpecFor("arg0_uuid");
    assertEquals(input.getDataType(), FieldSpec.DataType.UUID);
    assertFalse(input.isVirtualColumn());
    FieldSpec result = schema.getFieldSpecFor("result_uuid");
    assertEquals(result.getDataType(), FieldSpec.DataType.UUID);
    assertFalse(result.isVirtualColumn());
  }

  private static class TestUuidUdf extends Udf {
    @Override
    public String getMainName() {
      return "TEST_UUID";
    }

    @Override
    public String getDescription() {
      return "Test-only UUID identity function.";
    }

    @Override
    public Map<UdfSignature, Set<UdfExample>> getExamples() {
      UdfSignature signature = UdfSignature.of(
          UdfParameter.of("input", FieldSpec.DataType.UUID),
          UdfParameter.result(FieldSpec.DataType.UUID));
      return UdfExampleBuilder.forSignature(signature)
          .addExample("identity", "550e8400-e29b-41d4-a716-446655440000",
              "550e8400-e29b-41d4-a716-446655440000")
          .build()
          .generateExamples();
    }

    @Nullable
    @Override
    public PinotScalarFunction getScalarFunction() {
      return null;
    }
  }
}
