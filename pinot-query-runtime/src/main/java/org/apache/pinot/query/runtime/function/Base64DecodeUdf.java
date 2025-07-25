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
 * Unless required by applicable law or agreed in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.query.runtime.function;

import com.google.auto.service.AutoService;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.function.scalar.DataTypeConversionFunctions;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;

@AutoService(Udf.class)
public class Base64DecodeUdf extends Udf.FromAnnotatedMethod {
  public Base64DecodeUdf() throws NoSuchMethodException {
    super(DataTypeConversionFunctions.class.getMethod("base64Decode", byte[].class));
  }

  @Override
  public String getDescription() {
    return "Decodes a Base64-encoded bytes into a another bytes. "
        + "If the input is null or empty, returns null or an empty bytes respectively.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    // TODO: The UdfFramework does not correctly report results when the output is a byte array.
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("input", FieldSpec.DataType.BYTES)
                .withDescription("Base64-encoded input byte array"),
            UdfParameter.result(FieldSpec.DataType.BYTES)
                .withDescription("Decoded byte array")
        ))
        .addExample("decode AQID", "AQID".getBytes(), new byte[]{1, 2, 3})
        .addExample("decode empty array", new byte[]{}, new byte[]{})
        .addExample(UdfExample.create("null input", null, null).withoutNull(new byte[]{}))
        .build()
        .generateExamples();
  }
}
