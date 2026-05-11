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
package org.apache.pinot.common.partition.function;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.pinot.common.function.scalar.HashFunctions;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.segment.spi.partition.PartitionFunctionFactory;
import org.apache.pinot.segment.spi.partition.PartitionIdNormalizer;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;


public class CustomPartitionFunctionTest {
  private static final int NUM_PARTITIONS = 128;
  private static final String PARTITION_COLUMN = "id";

  @Test
  public void testPartitionExpressionWithScalarFunctions() {
    String expression = "fnv1aHash32UTF8(md5(toUtf8(id)))";
    PartitionFunction partitionFunction = getPartitionFunction(expression);

    String value = "user-1";
    int expected = PartitionIdNormalizer.POSITIVE_MODULO.getPartitionId(
        HashFunctions.fnv1aHash32UTF8(HashFunctions.md5(value.getBytes(StandardCharsets.UTF_8))), NUM_PARTITIONS);

    assertTrue(partitionFunction instanceof CustomPartitionFunction);
    assertEquals(partitionFunction.getName(), CustomPartitionFunction.NAME);
    assertEquals(partitionFunction.getFunctionConfig().get(CustomPartitionFunction.PARTITION_EXPRESSION_CONFIG_KEY),
        expression);
    assertEquals(partitionFunction.getPartition(value), expected);
  }

  @Test
  public void testPartitionExpressionNormalizesRawResults() {
    String expression = "plus(id, 0)";
    PartitionFunction partitionFunction = getPartitionFunction(expression);

    assertEquals(partitionFunction.getFunctionConfig().get(CustomPartitionFunction.PARTITION_EXPRESSION_CONFIG_KEY),
        expression);
    assertEquals(partitionFunction.getPartition("-1"), NUM_PARTITIONS - 1);
    assertEquals(partitionFunction.getPartition("128"), 0);
  }

  @Test
  public void testPartitionExpressionUsesConfiguredNormalizer() {
    int numPartitions = 127;
    PartitionFunction partitionFunction = PartitionFunctionFactory.getPartitionFunction(PARTITION_COLUMN,
        CustomPartitionFunction.NAME, numPartitions,
        Map.of(CustomPartitionFunction.PARTITION_EXPRESSION_CONFIG_KEY, "plus(id,0)",
            "partitionIdNormalizer", "MASK"));

    assertEquals(partitionFunction.getFunctionConfig().get(CustomPartitionFunction.PARTITION_EXPRESSION_CONFIG_KEY),
        "plus(id,0)");
    assertEquals(partitionFunction.getFunctionConfig().get("partitionIdNormalizer"), "MASK");
    assertEquals(partitionFunction.getPartition("-3"), PartitionIdNormalizer.MASK.getPartitionId(-3L, numPartitions));
  }

  @Test
  public void testRejectsNumPartitionsIdentifier() {
    assertThrows(IllegalArgumentException.class, () -> getPartitionFunction("plus(id, numPartitions)"));
  }

  @Test
  public void testPartitionExpressionReturnsInvalidPartitionForInvalidRuntimeResults() {
    assertEquals(getPartition("plus(id,0)", "not-a-number"), -1);
  }

  @Test
  public void testRejectsNonNumericPartitionExpression() {
    assertThrows(IllegalArgumentException.class, () -> getPartitionFunction("md5(id)"));
  }

  @Test
  public void testRejectsNonFunctionPartitionExpression() {
    assertThrows(IllegalArgumentException.class, () -> getPartitionFunction("id"));
  }

  @Test
  public void testCanUseCustomFunctionNameDirectly() {
    PartitionFunction partitionFunction = PartitionFunctionFactory.getPartitionFunction(PARTITION_COLUMN,
        CustomPartitionFunction.NAME, NUM_PARTITIONS, Map.of(CustomPartitionFunction.PARTITION_EXPRESSION_CONFIG_KEY,
            "plus(id,0)"));

    assertTrue(partitionFunction instanceof CustomPartitionFunction);
    assertEquals(partitionFunction.getFunctionConfig().get(CustomPartitionFunction.PARTITION_EXPRESSION_CONFIG_KEY),
        "plus(id,0)");
    assertEquals(partitionFunction.getPartition("-1"), NUM_PARTITIONS - 1);
  }

  @Test
  public void testRejectsMissingConfigForCustomFunction() {
    assertThrows(IllegalArgumentException.class,
        () -> PartitionFunctionFactory.getPartitionFunction(PARTITION_COLUMN, CustomPartitionFunction.NAME,
            NUM_PARTITIONS, null));
    assertThrows(IllegalArgumentException.class,
        () -> PartitionFunctionFactory.getPartitionFunction(CustomPartitionFunction.NAME, NUM_PARTITIONS,
            Map.of(CustomPartitionFunction.PARTITION_EXPRESSION_CONFIG_KEY, "plus(id,0)")));
  }

  @Test
  public void testRejectsColumnThatDoesNotMatchColumnPartitionMapKey() {
    assertThrows(IllegalArgumentException.class, () -> getPartitionFunction("plus(other,0)"));
  }

  @Test
  public void testRejectsExpressionWithoutPartitionColumn() {
    assertThrows(IllegalArgumentException.class, () -> getPartitionFunction("plus(1, 0)"));
  }

  @Test
  public void testRejectsMultiplePartitionColumns() {
    assertThrows(IllegalArgumentException.class,
        () -> PartitionFunctionFactory.getPartitionFunction(PARTITION_COLUMN, CustomPartitionFunction.NAME,
            NUM_PARTITIONS,
            Map.of(CustomPartitionFunction.PARTITION_EXPRESSION_CONFIG_KEY, "plus(id,other)")));
  }

  @Test
  public void testDelegatesFunctionBindingToInbuiltFunctionEvaluator() {
    assertTrue(getPartitionFunction("fnv1aHash32UTF8(md5(id))") instanceof CustomPartitionFunction);
    assertThrows(RuntimeException.class, () -> getPartitionFunction("missingScalarFunction(id)"));
  }

  @Test
  public void testRejectsNonDeterministicExpression() {
    assertThrows(IllegalArgumentException.class, () -> getPartitionFunction("plus(id, rand())"));
  }

  private static int getPartition(String expression, String value) {
    return getPartitionFunction(expression).getPartition(value);
  }

  private static PartitionFunction getPartitionFunction(String expression) {
    return PartitionFunctionFactory.getPartitionFunction(PARTITION_COLUMN, getColumnPartitionConfig(expression));
  }

  private static ColumnPartitionConfig getColumnPartitionConfig(String expression) {
    return new ColumnPartitionConfig(CustomPartitionFunction.NAME, NUM_PARTITIONS,
        Map.of(CustomPartitionFunction.PARTITION_EXPRESSION_CONFIG_KEY, expression));
  }
}
