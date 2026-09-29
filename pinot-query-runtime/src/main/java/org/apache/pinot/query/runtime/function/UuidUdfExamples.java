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

import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.UuidUtils;


/// Shared deterministic inputs and signatures for UUID UDF examples.
///
/// This class is stateless and thread-safe.
final class UuidUdfExamples {
  static final String UUID_V4 = "550e8400-e29b-41d4-a716-446655440000";
  static final String UUID_V7 = "017f22e2-79b0-7cc3-98c4-dc0c0c07398f";
  static final long UUID_V7_TIMESTAMP = 1_645_557_742_000L;

  private UuidUdfExamples() {
  }

  static byte[] bytes(String value) {
    return UuidUtils.toBytes(value);
  }

  static UdfExampleBuilder.SingleBuilder builder(FieldSpec.DataType inputType, FieldSpec.DataType resultType) {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
        UdfParameter.of("input", inputType).withDescription("UUID value represented as " + inputType),
        UdfParameter.result(resultType).withDescription("Function result")));
  }
}
