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
package org.apache.pinot.query.runtime.function;

import java.util.List;
import org.apache.pinot.core.udf.Udf;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


/// Tests the UUID UDF metadata and deterministic example coverage.
public class UuidUdfTest {
  @Test
  public void testDeterministicUuidUdfsHaveExamples()
      throws NoSuchMethodException {
    List<Udf> udfs = List.of(
        new BytesToUuidUdf(),
        new IsUuidUdf(),
        new ToUuidUdf(),
        new UuidTimestampUdf(),
        new UuidToBytesUdf(),
        new UuidToStringUdf(),
        new UuidVersionUdf());

    for (Udf udf : udfs) {
      assertFalse(udf.getMainName().isEmpty());
      assertFalse(udf.getAllNames().isEmpty());
      assertFalse(udf.getDescription().isEmpty());
      assertFalse(udf.getExamples().isEmpty(), udf.getMainName() + " must provide deterministic examples");
      assertNotNull(udf.getScalarFunction());
    }
  }

  @Test
  public void testNonDeterministicUuidGeneratorsSkipExactResultExamples()
      throws NoSuchMethodException {
    List<Udf> generators = List.of(new UuidV4Udf(), new UuidV7Udf());

    for (Udf generator : generators) {
      assertFalse(generator.getMainName().isEmpty());
      assertFalse(generator.getDescription().isEmpty());
      assertTrue(generator.getExamples().isEmpty());
      assertNotNull(generator.getScalarFunction());
    }
  }
}
