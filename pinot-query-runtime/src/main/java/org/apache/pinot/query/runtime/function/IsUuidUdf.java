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
import org.apache.pinot.common.function.scalar.uuid.IsUuidScalarFunction;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;


/// Multi-stage wrapper for validating STRING, BYTES, or UUID values.
///
/// This implementation is stateless and thread-safe.
@AutoService(Udf.class)
public class IsUuidUdf extends Udf {
  private static final IsUuidScalarFunction SCALAR_FUNCTION = new IsUuidScalarFunction();

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
    return "Returns true when the input is a valid RFC 4122 UUID STRING, 16-byte BYTES, or UUID value.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(
            UdfSignature.of(
                UdfParameter.of("input", FieldSpec.DataType.STRING)
                    .withDescription("Candidate UUID text, in canonical RFC 4122 form or as 32 hex digits"),
                UdfParameter.result(FieldSpec.DataType.BOOLEAN)
                    .withDescription("True when the input parses as a UUID")
            ))
        .addExample("canonical", "550e8400-e29b-41d4-a716-446655440000", true)
        .addExample("upper case", "550E8400-E29B-41D4-A716-446655440000", true)
        .addExample("hex without dashes", "550e8400e29b41d4a716446655440000", true)
        .addExample("not a uuid", "hello", false)
        .addExample("empty", "", false)
        // With null handling disabled the column falls back to the default null value for STRING ("null"),
        // which is not a UUID.
        .addExample(UdfExample.create("null input", null, null).withoutNull(false))
        .and(UdfExampleBuilder.forSignature(
                UdfSignature.of(
                    UdfParameter.of("input", FieldSpec.DataType.BYTES)
                        .withDescription("Candidate UUID bytes; valid when exactly 16 bytes wide"),
                    UdfParameter.result(FieldSpec.DataType.BOOLEAN)
                        .withDescription("True when the input is exactly 16 bytes")
                ))
            .addExample("sixteen bytes", new byte[]{
                85, 14, -124, 0, -30, -101, 65, -44, -89, 22, 68, 102, 85, 68, 0, 0
            }, true)
            .addExample("too short", new byte[]{1, 2, 3, 4}, false)
            // With null handling disabled the column falls back to the default null value for BYTES
            // (zero-length), which is not 16 bytes wide.
            .addExample(UdfExample.create("null input", null, null).withoutNull(false))
            .build())
        .and(UuidUdfExamples.builder(FieldSpec.DataType.UUID, FieldSpec.DataType.BOOLEAN)
            .addExample("logical uuid", UuidUdfExamples.UUID_V4, true)
            // With null handling disabled, UUID uses the nil UUID as its default value, which remains valid.
            .addExample(UdfExample.create("null input", null, null).withoutNull(true))
            .build())
        .generateExamples();
  }

  @Override
  public PinotScalarFunction getScalarFunction() {
    return SCALAR_FUNCTION;
  }
}
