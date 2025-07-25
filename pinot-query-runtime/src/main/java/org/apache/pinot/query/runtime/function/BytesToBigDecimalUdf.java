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
import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.function.scalar.ArithmeticFunctions;
import org.apache.pinot.common.function.scalar.DataTypeConversionFunctions;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.BigDecimalUtils;


@AutoService(Udf.class)
public class BytesToBigDecimalUdf extends Udf.FromAnnotatedMethod {

  public BytesToBigDecimalUdf()
      throws NoSuchMethodException {
    super(DataTypeConversionFunctions.class.getMethod("bytesToBigDecimal", byte[].class));
  }

  @Override
  public String getDescription() {
    return "Converts a BYTES to a BIG_DECIMAL. \n"
        + "The expected format is:\n"
        + " - First 2 bytes: scale (big-endian, unsigned short)\n"
        + " - Remaining bytes: unscaled value (big-endian two's complement, as per `BigInteger.toByteArray()`)\n";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("bytes", FieldSpec.DataType.BYTES)
                .withDescription("Byte array to convert to BigDecimal"),
            UdfParameter.result(FieldSpec.DataType.BIG_DECIMAL)
        ))
        .addExample("BigDecimal 1.23", BigDecimalUtils.serialize(new BigDecimal("1.23")), new BigDecimal("1.23"))
        //.addExample(UdfExample.create("null input", null, null).withoutNull(new BigDecimal("0.0")))
        .build()
        .generateExamples();
  }
}
