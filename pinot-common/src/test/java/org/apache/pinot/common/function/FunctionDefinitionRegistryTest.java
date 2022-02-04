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
package org.apache.pinot.common.function;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class FunctionDefinitionRegistryTest {

  @Test
  public void testIsAggFunc() {
    assertTrue(FunctionDefinitionRegistry.isAggFunc("count"));
    assertTrue(FunctionDefinitionRegistry.isAggFunc("percentileRawEstMV"));
    assertTrue(FunctionDefinitionRegistry.isAggFunc("PERCENTILERAWESTMV"));
    assertTrue(FunctionDefinitionRegistry.isAggFunc("percentilerawestmv"));
    assertTrue(FunctionDefinitionRegistry.isAggFunc("percentile_raw_est_mv"));
    assertTrue(FunctionDefinitionRegistry.isAggFunc("PERCENTILE_RAW_EST_MV"));
    assertTrue(FunctionDefinitionRegistry.isAggFunc("PERCENTILEEST90"));
    assertTrue(FunctionDefinitionRegistry.isAggFunc("percentileest90"));
    assertFalse(FunctionDefinitionRegistry.isAggFunc("toEpochSeconds"));
  }

  @Test
  public void testIsTransformFunc() {
    assertTrue(FunctionDefinitionRegistry.isTransformFunc("toEpochSeconds"));
    assertTrue(FunctionDefinitionRegistry.isTransformFunc("json_extract_scalar"));
    assertTrue(FunctionDefinitionRegistry.isTransformFunc("jsonextractscalar"));
    assertTrue(FunctionDefinitionRegistry.isTransformFunc("JSON_EXTRACT_SCALAR"));
    assertTrue(FunctionDefinitionRegistry.isTransformFunc("JSONEXTRACTSCALAR"));
    assertTrue(FunctionDefinitionRegistry.isTransformFunc("jsonExtractScalar"));
    assertTrue(FunctionDefinitionRegistry.isTransformFunc("ST_AsText"));
    assertTrue(FunctionDefinitionRegistry.isTransformFunc("STAsText"));
    assertTrue(FunctionDefinitionRegistry.isTransformFunc("stastext"));
    assertTrue(FunctionDefinitionRegistry.isTransformFunc("ST_ASTEXT"));
    assertTrue(FunctionDefinitionRegistry.isTransformFunc("STASTEXT"));
    assertFalse(FunctionDefinitionRegistry.isTransformFunc("foo_bar"));
  }
}
