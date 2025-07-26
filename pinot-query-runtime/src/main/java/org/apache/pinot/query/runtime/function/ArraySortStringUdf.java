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
public class ArraySortStringUdf extends Udf.FromAnnotatedMethod {
  public ArraySortStringUdf() throws NoSuchMethodException {
    super(ArrayFunctions.class.getMethod("arraySortString", String[].class));
  }

  @Override
  public String getDescription() {
    return "Sorts the input array of strings in ascending (lexicographical) order. "
        + "If the array is null, returns null. If the array is empty, returns an empty array.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("array", FieldSpec.DataType.STRING)
                .asMultiValued()
                .withDescription("Input array of strings"),
            UdfParameter.result(FieldSpec.DataType.STRING)
                .asMultiValued()
                .withDescription("Sorted array in ascending (lexicographical) order")
        ))
        .addExample("sort array", List.of("c", "a", "b"), List.of("a", "b", "c"))
        .addExample("sort array with duplicates", List.of("b", "a", "b", "c"), List.of("a", "b", "b", "c"))
        .addExample("sort already sorted", List.of("a", "b", "c"), List.of("a", "b", "c"))
        .addExample("sort single element", List.of("x"), List.of("x"))
        .addExample("sort empty array", List.of(), List.of())
        .addExample(UdfExample.create("null array", null, null).withoutNull(List.of()))
        .build()
        .generateExamples();
  }
}
