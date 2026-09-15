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
import org.apache.pinot.common.function.scalar.uuid.UuidToStringScalarFunction;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;


/// Multi-stage wrapper for rendering STRING, BYTES, or UUID values as canonical UUID text.
///
/// This implementation is stateless and thread-safe.
@AutoService(Udf.class)
public class UuidToStringUdf extends Udf {
  private static final UuidToStringScalarFunction SCALAR_FUNCTION = new UuidToStringScalarFunction();

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
    return "Converts a STRING, BYTES, or UUID value to its canonical lowercase RFC 4122 string representation.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UuidUdfExamples.builder(FieldSpec.DataType.STRING, FieldSpec.DataType.STRING)
        .addExample("upper case string", UuidUdfExamples.UUID_V4.toUpperCase(), UuidUdfExamples.UUID_V4)
        .and(UuidUdfExamples.builder(FieldSpec.DataType.BYTES, FieldSpec.DataType.STRING)
            .addExample("canonical bytes", UuidUdfExamples.bytes(UuidUdfExamples.UUID_V4), UuidUdfExamples.UUID_V4)
            .build())
        .and(UuidUdfExamples.builder(FieldSpec.DataType.UUID, FieldSpec.DataType.STRING)
            .addExample("logical uuid", UuidUdfExamples.UUID_V4, UuidUdfExamples.UUID_V4)
            .build())
        .generateExamples();
  }

  @Override
  public PinotScalarFunction getScalarFunction() {
    return SCALAR_FUNCTION;
  }
}
