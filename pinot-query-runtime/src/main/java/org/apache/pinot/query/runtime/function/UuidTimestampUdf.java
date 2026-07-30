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
import org.apache.pinot.common.function.scalar.uuid.UuidTimestampScalarFunction;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;


/// Multi-stage wrapper for extracting Unix milliseconds from time-based STRING, BYTES, or UUID values.
///
/// This implementation is stateless and thread-safe.
@AutoService(Udf.class)
public class UuidTimestampUdf extends Udf {
  private static final UuidTimestampScalarFunction SCALAR_FUNCTION = new UuidTimestampScalarFunction();

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
    return "Returns the embedded Unix-millisecond timestamp from a time-based STRING, BYTES, or UUID value "
        + "(version 1, 6, or 7). Throws for non-time-based versions.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UuidUdfExamples.builder(FieldSpec.DataType.STRING, FieldSpec.DataType.LONG)
        .addExample("version 7 string", UuidUdfExamples.UUID_V7, UuidUdfExamples.UUID_V7_TIMESTAMP)
        .and(UuidUdfExamples.builder(FieldSpec.DataType.BYTES, FieldSpec.DataType.LONG)
            .addExample("version 7 bytes", UuidUdfExamples.bytes(UuidUdfExamples.UUID_V7),
                UuidUdfExamples.UUID_V7_TIMESTAMP)
            .build())
        .and(UuidUdfExamples.builder(FieldSpec.DataType.UUID, FieldSpec.DataType.LONG)
            .addExample("version 7 uuid", UuidUdfExamples.UUID_V7, UuidUdfExamples.UUID_V7_TIMESTAMP)
            .build())
        .generateExamples();
  }

  @Override
  public PinotScalarFunction getScalarFunction() {
    return SCALAR_FUNCTION;
  }
}
