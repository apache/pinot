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
package org.apache.pinot.segment.spi.partition;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import org.apache.pinot.segment.spi.partition.pipeline.PartitionPipelineFunction;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.function.FunctionEvaluator;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Integration tests for expression-mode partition functions that require `pinot-common` on the classpath
/// (for [org.apache.pinot.common.evaluator.InbuiltPartitionEvaluatorFactory]).
public class PartitionFunctionExprIntegrationTest {

  @Test
  public void testFunctionExprPartitionFunctionImplementsFunctionEvaluator() {
    PartitionFunction partitionFunction =
        PartitionFunctionFactory.getPartitionFunction("id", null, 128, null, "positiveModulo(fnv1a_32(md5(id)), 128)");
    assertTrue(partitionFunction instanceof FunctionEvaluator);
    FunctionEvaluator evaluator = (FunctionEvaluator) partitionFunction;
    GenericRow row = new GenericRow();
    row.putValue("id", "000016be-9d72-466c-9632-cfa680dc8fa3");

    assertEquals(evaluator.getArguments(), List.of("id"));
    int directPartition = partitionFunction.getPartition("000016be-9d72-466c-9632-cfa680dc8fa3");
    assertTrue(directPartition >= 0 && directPartition < 128, "partition id must be in [0, 128)");
    assertEquals(evaluator.evaluate(row), directPartition);
    assertEquals(evaluator.evaluate(new Object[]{"000016be-9d72-466c-9632-cfa680dc8fa3"}), directPartition);
  }

  @Test
  public void testFunctionExprPartitionFunctionSerialization() {
    PartitionFunction partitionFunction =
        PartitionFunctionFactory.getPartitionFunction("id", null, 128, null, "positiveModulo(fnv1a_32(md5(id)), 128)");

    JsonNode jsonNode = JsonUtils.objectToJsonNode(partitionFunction);
    assertEquals(partitionFunction.getName(), PartitionPipelineFunction.NAME);
    assertEquals(jsonNode.get("name").asText(), PartitionPipelineFunction.NAME);
    assertEquals(jsonNode.get("numPartitions").asInt(), 128);
    assertEquals(jsonNode.get("functionExpr").asText(), "positivemodulo(fnv1a_32(md5(id)), 128)");
  }
}
