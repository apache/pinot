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
import org.apache.pinot.common.function.scalar.uuid.UuidToBytesScalarFunction;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;


/// Multi-stage wrapper for converting STRING, BYTES, or UUID values to canonical UUID bytes.
///
/// This implementation is stateless and thread-safe.
@AutoService(Udf.class)
public class UuidToBytesUdf extends Udf {
  private static final UuidToBytesScalarFunction SCALAR_FUNCTION = new UuidToBytesScalarFunction();

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
    return "Converts a STRING, BYTES, or UUID value to its canonical 16-byte UUID representation.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    byte[] bytes = UuidUdfExamples.bytes(UuidUdfExamples.UUID_V4);
    return UuidUdfExamples.builder(FieldSpec.DataType.STRING, FieldSpec.DataType.BYTES)
        .addExample("canonical string", UuidUdfExamples.UUID_V4, bytes)
        .and(UuidUdfExamples.builder(FieldSpec.DataType.BYTES, FieldSpec.DataType.BYTES)
            .addExample("canonical bytes", bytes, bytes)
            .build())
        .and(UuidUdfExamples.builder(FieldSpec.DataType.UUID, FieldSpec.DataType.BYTES)
            .addExample("logical uuid", UuidUdfExamples.UUID_V4, bytes)
            .build())
        .generateExamples();
  }

  @Override
  public PinotScalarFunction getScalarFunction() {
    return SCALAR_FUNCTION;
  }
}
