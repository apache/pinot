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
public class ArrayElementAtFloatUdf extends Udf.FromAnnotatedMethod {
  public ArrayElementAtFloatUdf() throws NoSuchMethodException {
    super(ArrayFunctions.class.getMethod("arrayElementAtFloat", float[].class, int.class));
  }

  @Override
  public String getDescription() {
    return "Returns the element at the specified index in an array of floats. "
        + "The index is 1-based, meaning that the first element is at index 1. ";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("array", FieldSpec.DataType.FLOAT)
                .asMultiValued()
                .withDescription("Array of floats"),
            UdfParameter.of("index", FieldSpec.DataType.INT)
                .withDescription("One-based index of the element to retrieve"),
            UdfParameter.result(FieldSpec.DataType.FLOAT)
                .withDescription("Element at the specified index or 0 if index is out of bounds. If any argument is null, returns null or 0.")
        ))
        .addExample("middle element", List.of(1.1f, 2.2f, 3.3f), 2, 2.2f)
        .addExample("first element", List.of(1.1f, 2.2f, 3.3f), 1, 1.1f)
        .addExample("last element", List.of(1.1f, 2.2f, 3.3f), 3, 3.3f)
        .addExample("out of bounds index", List.of(1.1f, 2.2f), 3, 0f)
        .addExample("negative index", List.of(1.1f, 2.2f), -1, 0f)
        .addExample("zero index", List.of(1.1f, 2.2f), 0, 0f)
        .addExample(UdfExample.create("empty array", List.of(), 0, null).withoutNull(0f))
        .addExample(UdfExample.create("null array", null, 0, null).withoutNull(0f))
        .addExample(UdfExample.create("null index", List.of(1.1f, 2.2f), null, null).withoutNull(0f))
        .addExample(UdfExample.create("null input", null, null, null).withoutNull(0f))
        .build()
        .generateExamples();
  }
}
