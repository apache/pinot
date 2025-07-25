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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.function.scalar.ArrayFunctions;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;

@AutoService(Udf.class)
public class ArraySumLongUdf extends Udf.FromAnnotatedMethod {
  public ArraySumLongUdf() throws NoSuchMethodException {
    super(ArrayFunctions.class.getMethod("arraySumLong", long[].class));
  }

  @Override
  public String getDescription() {
    return "Returns the sum of all elements in the input array of longs. "
        + "If the array is null, returns null. If the array is empty, returns 0.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("array", FieldSpec.DataType.LONG)
                .asMultiValued()
                .withDescription("Input array of longs"),
            UdfParameter.result(FieldSpec.DataType.LONG)
                .withDescription("Sum of all elements in the array")
        ))
        .addExample("sum array", List.of(1L, 2L, 3L), 6L)
        .addExample("sum array with negatives", List.of(-1L, 2L, -3L), -2L)
        .addExample("sum single element", List.of(42L), 42L)
        .addExample("sum empty array", List.of(), 0L)
        .addExample(UdfExample.create("null array", null, null).withoutNull(0L))
        .build()
        .generateExamples();
  }
}
