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
package org.apache.pinot.spi.data;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Map;
import java.util.UUID;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class OpenStructTypeInferenceTest {

  @DataProvider(name = "inferenceCases")
  public Object[][] inferenceCases() {
    return new Object[][]{
        // Integral types all widen to INT.
        {42, DataType.INT},
        {(byte) 1, DataType.INT},
        {'a', DataType.INT},
        {(short) 7, DataType.INT},
        {42L, DataType.LONG},
        {3.14f, DataType.FLOAT},
        {3.14d, DataType.DOUBLE},
        {new BigDecimal("1.23"), DataType.BIG_DECIMAL},
        {true, DataType.BOOLEAN},
        {new Timestamp(0L), DataType.TIMESTAMP},
        {"hello", DataType.STRING},
        // Temporal and UUID values fold to STRING.
        {LocalDate.of(2026, 6, 2), DataType.STRING},
        {LocalTime.of(12, 0), DataType.STRING},
        {UUID.randomUUID(), DataType.STRING},
        {new byte[]{1, 2, 3}, DataType.BYTES},
        // Unrepresentable values return null so callers can drop or default.
        {Map.of("k", "v"), null},
        {new Object(), null},
    };
  }

  @Test(dataProvider = "inferenceCases")
  public void testInferDataType(Object rawValue, DataType expected) {
    assertEquals(OpenStructTypeInference.inferDataType(rawValue), expected);
  }
}
