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
package org.apache.pinot.segment.spi.partition.metadata;

import java.util.Set;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.segment.spi.partition.pipeline.PartitionFunctionExprCompiler;
import org.apache.pinot.segment.spi.partition.pipeline.PartitionPipelineFunction;
import org.apache.pinot.segment.spi.partition.pipeline.PartitionValueType;
import org.apache.pinot.spi.utils.BytesUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;


public class ColumnPartitionMetadataTest {
  @Test
  public void testRoundTripTreatsNullExpressionFieldsAsAbsent()
      throws Exception {
    ColumnPartitionMetadata metadata = new ColumnPartitionMetadata("Modulo", 8, Set.of(3), null);

    ColumnPartitionMetadata roundTripped =
        JsonUtils.stringToObject(JsonUtils.objectToString(metadata), ColumnPartitionMetadata.class);

    assertEquals(roundTripped.getFunctionName(), metadata.getFunctionName());
    assertEquals(roundTripped.getNumPartitions(), metadata.getNumPartitions());
    assertEquals(roundTripped.getPartitions(), metadata.getPartitions());
    assertNull(roundTripped.getFunctionExpr());
    assertEquals(roundTripped, metadata);
  }

  @Test
  public void testConstructorFromPartitionFunctionPreservesExpressionFields() {
    ColumnPartitionMetadata metadata = new ColumnPartitionMetadata(
        PartitionFunctionExprCompiler.compilePartitionFunction("id", "positiveModulo(fnv1a_32(md5(id)), 128)", 128),
        Set.of(104));

    // Expression-mode: functionName is the stable "FunctionExpr" sentinel for backward compatibility.
    // Old brokers that call jsonMetadata.get("functionName").asText() without a null guard will receive
    // this non-null value and fail with IllegalArgumentException (graceful degradation: no pruning).
    assertEquals(metadata.getFunctionName(), PartitionPipelineFunction.NAME);
    assertEquals(metadata.getNumPartitions(), 128);
    assertEquals(metadata.getPartitions(), Set.of(104));
    assertEquals(metadata.getFunctionExpr(), "positivemodulo(fnv1a_32(md5(id)), 128)");
    assertNull(metadata.getInputType()); // STRING is the default, not stored
  }

  @Test
  public void testExpressionModeRoundTripPreservesAllFields()
      throws Exception {
    ColumnPartitionMetadata metadata = new ColumnPartitionMetadata(
        PartitionFunctionExprCompiler.compilePartitionFunction("id", "positiveModulo(fnv1a_32(md5(id)), 64)", 64),
        Set.of(7, 15));

    String json = JsonUtils.objectToString(metadata);

    // The serialized JSON must contain "functionName" so old brokers do not NPE when they call
    // jsonMetadata.get("functionName").asText() without a null check.
    assertTrue(json.contains("\"functionName\""), "JSON must include functionName for backward compatibility");
    assertTrue(json.contains("\"functionExpr\""), "JSON must include functionExpr");

    ColumnPartitionMetadata roundTripped = JsonUtils.stringToObject(json, ColumnPartitionMetadata.class);

    assertEquals(roundTripped.getFunctionName(), PartitionPipelineFunction.NAME);
    assertEquals(roundTripped.getFunctionExpr(), "positivemodulo(fnv1a_32(md5(id)), 64)");
    assertEquals(roundTripped.getNumPartitions(), 64);
    assertEquals(roundTripped.getPartitions(), Set.of(7, 15));
    assertNull(roundTripped.getInputType());
    assertEquals(roundTripped, metadata);
  }

  @Test
  public void testBytesInputTypeRoundTripRebuildsMatchingPartitionFunction()
      throws Exception {
    byte[] value = new byte[]{1, 2, 3};
    PartitionPipelineFunction partitionFunction = PartitionFunctionExprCompiler.compilePartitionFunction(
        "id", PartitionValueType.BYTES, "positiveModulo(murmur2(id), 16)", 16);
    ColumnPartitionMetadata metadata =
        new ColumnPartitionMetadata(partitionFunction, Set.of(partitionFunction.getPartition(value)));

    String json = JsonUtils.objectToString(metadata);
    assertTrue(json.contains("\"partitionInputType\""), "JSON must include non-default BYTES input type");

    ColumnPartitionMetadata roundTripped = JsonUtils.stringToObject(json, ColumnPartitionMetadata.class);
    PartitionFunction rebuilt = PartitionFunctionFactory.getPartitionFunction("id", roundTripped);

    assertEquals(roundTripped.getInputType(), PartitionValueType.BYTES.name());
    assertEquals(rebuilt.getPartition(BytesUtils.toHexString(value)), partitionFunction.getPartition(value));
  }
}
