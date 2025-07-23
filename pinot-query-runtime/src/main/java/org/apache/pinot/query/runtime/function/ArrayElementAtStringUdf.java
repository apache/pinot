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
public class ArrayElementAtStringUdf extends Udf.FromAnnotatedMethod {
  public ArrayElementAtStringUdf() throws NoSuchMethodException {
    super(ArrayFunctions.class.getMethod("arrayElementAtString", String[].class, int.class));
  }

  @Override
  public String getDescription() {
    return "Returns the element at the specified index in an array of strings. The index is 1-based.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("array", FieldSpec.DataType.STRING)
                .asMultiValued()
                .withDescription("Array of strings"),
            UdfParameter.of("index", FieldSpec.DataType.INT)
                .withDescription("One-based index of the element to retrieve"),
            UdfParameter.result(FieldSpec.DataType.STRING)
                .withDescription("Element at the specified index or empty string if index is out of bounds. If any argument is null, returns null or empty string.")
        ))
        .addExample("middle element", List.of("a", "b", "c"), 2, "b")
        .addExample("first element", List.of("a", "b", "c"), 1, "a")
        .addExample("last element", List.of("a", "b", "c"), 3, "c")
        .addExample("out of bounds index", List.of("a", "b"), 3, "")
        .addExample("negative index", List.of("a", "b"), -1, "")
        .addExample("zero index", List.of("a", "b"), 0, "")
        .addExample(UdfExample.create("empty array", List.of(), 0, null).withoutNull(""))
        .addExample(UdfExample.create("null array", null, 0, null).withoutNull(""))
        .addExample(UdfExample.create("null index", List.of("a", "b"), null, null).withoutNull(""))
        .addExample(UdfExample.create("null input", null, null, null).withoutNull(""))
        .build()
        .generateExamples();
  }
}
