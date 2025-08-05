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
import org.apache.pinot.common.function.scalar.HashFunctions;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * UDF stub for adler32 (not implemented).
 */
@AutoService(Udf.class)
public class Adler32Udf extends Udf.FromAnnotatedMethod {

  public Adler32Udf()
      throws NoSuchMethodException {
    super(HashFunctions.class.getMethod("adler32", byte[].class));
  }

  @Override
  public String getDescription() {
    return "Computes the Adler-32 checksum of a byte array. "
        + "Adler-32 is a checksum algorithm that is simple and fast, but not cryptographically secure.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(
            UdfSignature.of(
                UdfParameter.of("input", FieldSpec.DataType.BYTES)
                    .withDescription("Input byte array to compute the Adler-32 checksum"),
                UdfParameter.result(FieldSpec.DataType.INT)
                    .withDescription("The Adler-32 checksum of the input byte array")
            ))
        .addExample("empty", new byte[0], 1)
        .addExample("single byte", new byte[]{1}, 131074)
        .addExample("multiple bytes", new byte[]{1, 2, 3, 4}, 1572875)
        .addExample(UdfExample.create("null input", null, null).withoutNull(1))
        .build()
        .generateExamples();
  }
}
