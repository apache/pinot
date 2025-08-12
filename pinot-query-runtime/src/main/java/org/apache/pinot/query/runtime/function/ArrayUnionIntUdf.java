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
public class ArrayUnionIntUdf extends Udf.FromAnnotatedMethod {
  public ArrayUnionIntUdf() throws NoSuchMethodException {
    super(ArrayFunctions.class.getMethod("arrayUnionInt", int[].class, int[].class));
  }

  @Override
  public String getDescription() {
    return "Returns the union of two input arrays of integers (distinct values, order not guaranteed). "
        + "If either array is null, returns null. If both arrays are empty, returns an empty array.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("array1", FieldSpec.DataType.INT)
                .asMultiValued()
                .withDescription("First input array of integers"),
            UdfParameter.of("array2", FieldSpec.DataType.INT)
                .asMultiValued()
                .withDescription("Second input array of integers"),
            UdfParameter.result(FieldSpec.DataType.INT)
                .asMultiValued()
                .withDescription("Union of both arrays (distinct values)")
        ))
        .addExample("union with overlap", List.of(1, 2, 3), List.of(3, 4, 5), List.of(1, 2, 3, 4, 5))
        .addExample("union with no overlap", List.of(1, 2), List.of(3, 4), List.of(1, 2, 3, 4))
        .addExample("union with duplicates", List.of(1, 2, 2), List.of(2, 3, 3), List.of(1, 2, 3))
        .addExample("union with empty array1", List.of(), List.of(1, 2), List.of(1, 2))
        .addExample("union with empty array2", List.of(1, 2), List.of(), List.of(1, 2))
        .addExample("union with both empty", List.of(), List.of(), List.of())
        .addExample(UdfExample.create("null array1", null, List.of(1, 2), null).withoutNull(List.of(1, 2)))
        .addExample(UdfExample.create("null array2", List.of(1, 2), null, null).withoutNull(List.of(1, 2)))
        .addExample(UdfExample.create("null both", null, null, null).withoutNull(List.of()))
        .build()
        .generateExamples();
  }
}
