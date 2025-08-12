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
public class ArrayElementAtLongUdf extends Udf.FromAnnotatedMethod {
  public ArrayElementAtLongUdf() throws NoSuchMethodException {
    super(ArrayFunctions.class.getMethod("arrayElementAtLong", long[].class, int.class));
  }

  @Override
  public String getDescription() {
    return "Returns the element at the specified index in an array of longs. "
        + "The index is 1-based, meaning that the first element is at index 1. ";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("array", FieldSpec.DataType.LONG)
                .asMultiValued()
                .withDescription("Array of longs"),
            UdfParameter.of("index", FieldSpec.DataType.INT)
                .withDescription("One-based index of the element to retrieve"),
            UdfParameter.result(FieldSpec.DataType.LONG)
                .withDescription("Element at the specified index or 0 if index is out of bounds. "
                    + "If any argument is null, returns null or 0.")
        ))
        .addExample("middle element", List.of(10L, 20L, 30L), 2, 20L)
        .addExample("first element", List.of(10L, 20L, 30L), 1, 10L)
        .addExample("last element", List.of(10L, 20L, 30L), 3, 30L)
        .addExample("out of bounds index", List.of(10L, 20L), 3, 0L)
        .addExample("negative index", List.of(10L, 20L), -1, 0L)
        .addExample("zero index", List.of(10L, 20L), 0, 0L)
        .addExample(UdfExample.create("empty array", List.of(), 0, null).withoutNull(0L))
        .addExample(UdfExample.create("null array", null, 0, null).withoutNull(0L))
        .addExample(UdfExample.create("null index", List.of(10L, 20L), null, null).withoutNull(0L))
        .addExample(UdfExample.create("null input", null, null, null).withoutNull(0L))
        .build()
        .generateExamples();
  }
}
