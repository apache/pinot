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
import org.apache.pinot.common.function.scalar.ArrayFunctions;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;


@AutoService(Udf.class)
public class ArrayElementAtIntUdf extends Udf.FromAnnotatedMethod {
  public ArrayElementAtIntUdf()
      throws NoSuchMethodException {
    super(ArrayFunctions.class.getMethod("arrayElementAtInt", int[].class, int.class));
  }

  // language=markdown
  @Override
  public String getDescription() {
    return "Returns the element at the specified index in an array of integers. "
        + "The index is 1-based, meaning that the first element is at index 1. ";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("arr", FieldSpec.DataType.INT)
                .withDescription("Array of integers to retrieve the element from")
                .asMultiValued(),
            UdfParameter.of("idx", FieldSpec.DataType.INT)
                .withDescription("1-based index of the element to retrieve."),
            UdfParameter.result(FieldSpec.DataType.INT)
        ))
        .addExample("first element", new Integer[]{10, 20, 30}, 1, 10)
        .addExample("second element", new Integer[]{10, 20, 30}, 2, 20)
        .addExample("negative element", new Integer[]{10, 20, 30}, -1, 0) // Index < 1 returns 0
        .addExample("zero element", new Integer[]{10, 20, 30}, 0, 0) // Index < 1 returns 0
        .addExample("out of bounds element", new Integer[]{10, 20, 30}, 4, 0) // Index > length returns 0
        .build()
        .generateExamples();
  }
}
