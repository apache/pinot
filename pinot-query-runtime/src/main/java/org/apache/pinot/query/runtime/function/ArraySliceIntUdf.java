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
public class ArraySliceIntUdf extends Udf.FromAnnotatedMethod {
  public ArraySliceIntUdf() throws NoSuchMethodException {
    super(ArrayFunctions.class.getMethod("arraySliceInt", int[].class, int.class, int.class));
  }

  @Override
  public String getDescription() {
    return "Returns a slice of the input array of integers from start index (inclusive) to end index (exclusive). "
        + "Indexes are 0-based. If any argument is null, returns null. "
        + "If indexes are out of bounds, returns an empty array.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("array", FieldSpec.DataType.INT)
                .asMultiValued()
                .withDescription("Input array of integers"),
            UdfParameter.of("start", FieldSpec.DataType.INT)
                .withDescription("Start index (inclusive, 0-based)"),
            UdfParameter.of("end", FieldSpec.DataType.INT)
                .withDescription("End index (exclusive, 0-based)"),
            UdfParameter.result(FieldSpec.DataType.INT)
                .asMultiValued()
                .withDescription("Slice of the array from start to end index")
        ))
        .addExample("middle slice", List.of(1, 2, 3, 4, 5), 1, 4, List.of(2, 3, 4))
        .addExample("full slice", List.of(1, 2, 3), 0, 3, List.of(1, 2, 3))
        .addExample("empty slice", List.of(1, 2, 3), 2, 2, List.of())
        .addExample("out of bounds", List.of(1, 2, 3), 5, 10, List.of())
        .addExample("negative start", List.of(1, 2, 3), -1, 2, List.of())
        .addExample("negative end", List.of(1, 2, 3), 0, -1, List.of())
        .addExample("start greater than end", List.of(1, 2, 3), 2, 1, List.of())
        .addExample(UdfExample.create("null array", null, 0, 2, null).withoutNull(List.of()))
        .addExample(UdfExample.create("null start", List.of(1, 2, 3), null, 2, null)
            .withoutNull(List.of(1, 2)))
        .addExample(UdfExample.create("null end", List.of(1, 2, 3), 0, null, null).withoutNull(List.of()))
        .build()
        .generateExamples();
  }
}
