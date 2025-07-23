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
public class ArrayConcatStringUdf extends Udf.FromAnnotatedMethod {
  public ArrayConcatStringUdf() throws NoSuchMethodException {
    super(ArrayFunctions.class.getMethod("arrayConcatString", String[].class, String[].class));
  }

  @Override
  public String getDescription() {
    return "Concatenates two arrays of strings into a single array.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("value1", FieldSpec.DataType.STRING)
                .asMultiValued()
                .withDescription("First array to concatenate"),
            UdfParameter.of("value2", FieldSpec.DataType.STRING)
                .asMultiValued()
                .withDescription("Second array to concatenate"),
            UdfParameter.result(FieldSpec.DataType.STRING)
                .asMultiValued()
                .withDescription("Concatenated array")
        ))
        .addExample("two not empty arrays", List.of("a", "b"), List.of("c", "d"), List.of("a", "b", "c", "d"))
        .addExample("empty with not empty", List.of(), List.of("x", "y"), List.of("x", "y"))
        .addExample("not empty with empty", List.of("x", "y"), List.of(), List.of("x", "y"))
        .addExample("two empty arrays", List.of(), List.of(), List.of())
        .addExample(UdfExample.create("null concat not null", null, List.of("x", "y"), null)
            .withoutNull(List.of("x", "y")))
        .addExample(UdfExample.create("not null concat null", List.of("x", "y"), null, null)
            .withoutNull(List.of("x", "y")))
        .addExample(UdfExample.create("null input", null, null, null).withoutNull(List.of()))
        .build()
        .generateExamples();
  }
}
