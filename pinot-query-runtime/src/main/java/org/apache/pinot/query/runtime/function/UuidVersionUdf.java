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

import com.google.auto.service.AutoService;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.scalar.uuid.UuidVersionScalarFunction;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;


/// Multi-stage wrapper for extracting the version field from STRING, BYTES, or UUID values.
///
/// This implementation is stateless and thread-safe.
@AutoService(Udf.class)
public class UuidVersionUdf extends Udf {
  private static final UuidVersionScalarFunction SCALAR_FUNCTION = new UuidVersionScalarFunction();

  @Override
  public String getMainName() {
    return SCALAR_FUNCTION.getName();
  }

  @Override
  public Set<String> getAllNames() {
    return SCALAR_FUNCTION.getNames();
  }

  @Override
  public String getDescription() {
    return "Returns the 4-bit version field (0-15) of a STRING, BYTES, or UUID value. "
        + "Common values: 1, 3, 4, 5, 6, 7, 8.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UuidUdfExamples.builder(FieldSpec.DataType.STRING, FieldSpec.DataType.INT)
        .addExample("version 4 string", UuidUdfExamples.UUID_V4, 4)
        .and(UuidUdfExamples.builder(FieldSpec.DataType.BYTES, FieldSpec.DataType.INT)
            .addExample("version 4 bytes", UuidUdfExamples.bytes(UuidUdfExamples.UUID_V4), 4)
            .build())
        .and(UuidUdfExamples.builder(FieldSpec.DataType.UUID, FieldSpec.DataType.INT)
            .addExample("version 4 uuid", UuidUdfExamples.UUID_V4, 4)
            .build())
        .generateExamples();
  }

  @Override
  public PinotScalarFunction getScalarFunction() {
    return SCALAR_FUNCTION;
  }
}
