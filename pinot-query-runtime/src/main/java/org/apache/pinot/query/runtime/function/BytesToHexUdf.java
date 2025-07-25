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
import org.apache.pinot.common.function.scalar.DataTypeConversionFunctions;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.BytesUtils;


@AutoService(Udf.class)
public class BytesToHexUdf extends Udf.FromAnnotatedMethod {

  public BytesToHexUdf()
      throws NoSuchMethodException {
    super(DataTypeConversionFunctions.class.getMethod("bytesToHex", byte[].class));
  }

  @Override
  public String getDescription() {
    return "Converts a BYTES to a STRING using the hexadecimal representation.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("bytes", FieldSpec.DataType.BYTES)
                .withDescription("Byte array to convert to hexadecimal string"),
            UdfParameter.result(FieldSpec.DataType.STRING)
                .withDescription("Hexadecimal string representation of the byte array, in lowercase")
        ))
        .addExample("f012be3c", BytesUtils.toByteArray("f012be3c").getBytes(), "f012be3c")
        .addExample("cafebabe", BytesUtils.toByteArray("cafebabe").getBytes(), "cafebabe")
        .addExample("CAFEBABE", BytesUtils.toByteArray("CAFEBABE").getBytes(), "cafebabe")
        .addExample(UdfExample.create("null input", null, null).withoutNull(""))
        .build()
        .generateExamples();
  }
}
