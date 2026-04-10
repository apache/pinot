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
import org.apache.pinot.segment.spi.partition.pipeline.PartitionFunctionExprCompiler;
import org.apache.pinot.segment.spi.partition.pipeline.PartitionPipelineFunction;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


public class ColumnPartitionMetadataTest {
  @Test
  public void testRoundTripTreatsNullExpressionFieldsAsAbsent()
      throws Exception {
    ColumnPartitionMetadata metadata = new ColumnPartitionMetadata("Modulo", 8, Set.of(3), null);

    ColumnPartitionMetadata roundTripped =
        JsonUtils.stringToObject(JsonUtils.objectToString(metadata), ColumnPartitionMetadata.class);

    assertEquals(roundTripped, metadata);
    assertNull(roundTripped.getFunctionExpr());
    assertNull(roundTripped.getPartitionIdNormalizer());
  }

  @Test
  public void testConstructorFromPartitionFunctionPreservesExpressionFields() {
    PartitionPipelineFunction partitionFunction =
        PartitionFunctionExprCompiler.compilePartitionFunction("id", "fnv1a_32(md5(id))", 128, "MASK");

    ColumnPartitionMetadata metadata = new ColumnPartitionMetadata(partitionFunction, Set.of(104));

    assertEquals(metadata.getFunctionName(), PartitionPipelineFunction.NAME);
    assertEquals(metadata.getNumPartitions(), 128);
    assertEquals(metadata.getPartitions(), Set.of(104));
    assertEquals(metadata.getFunctionExpr(), "fnv1a_32(md5(id))");
    assertEquals(metadata.getPartitionIdNormalizer(), "MASK");
  }
}
