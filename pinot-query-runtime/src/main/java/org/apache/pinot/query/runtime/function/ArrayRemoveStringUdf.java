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
public class ArrayRemoveStringUdf extends Udf.FromAnnotatedMethod {
  public ArrayRemoveStringUdf() throws NoSuchMethodException {
    super(ArrayFunctions.class.getMethod("arrayRemoveString", String[].class, String.class));
  }

  @Override
  public String getDescription() {
    return "Removes the _first occurrence_ of the specified string from the input array.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("array", FieldSpec.DataType.STRING)
                .asMultiValued()
                .withDescription("Input array of strings"),
            UdfParameter.of("value", FieldSpec.DataType.STRING)
                .withDescription("String value to remove from array"),
            UdfParameter.result(FieldSpec.DataType.STRING)
                .asMultiValued()
                .withDescription("Array after removing the specified value")
        ))
        .addExample("remove present value", List.of("a", "b", "c", "b"), "b", List.of("a", "c", "b"))
        .addExample("remove absent value", List.of("a", "b", "c"), "d", List.of("a", "b", "c"))
        .addExample("remove from empty array", List.of(), "a", List.of())
        .addExample(UdfExample.create("remove on null array", null, "b", null).withoutNull(List.of()))
        .addExample(UdfExample.create("remove null value", List.of("a", "b", "c"), null, null)
            .withoutNull(List.of("a", "b", "c")))
        .addExample(UdfExample.create("remove null when array contains empty string",
                List.of("", "a", "b", "c"), null, List.of())
            .withoutNull(List.of("", "a", "b", "c")))
        .build()
        .generateExamples();
  }
}
