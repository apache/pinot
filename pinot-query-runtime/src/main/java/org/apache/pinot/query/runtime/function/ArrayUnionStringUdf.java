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
public class ArrayUnionStringUdf extends Udf.FromAnnotatedMethod {
  public ArrayUnionStringUdf() throws NoSuchMethodException {
    super(ArrayFunctions.class.getMethod("arrayUnionString", String[].class, String[].class));
  }

  @Override
  public String getDescription() {
    return "Returns the union of two input arrays of strings (distinct values, order not guaranteed). "
        + "If either array is null, returns null. If both arrays are empty, returns an empty array.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("array1", FieldSpec.DataType.STRING)
                .asMultiValued()
                .withDescription("First input array of strings"),
            UdfParameter.of("array2", FieldSpec.DataType.STRING)
                .asMultiValued()
                .withDescription("Second input array of strings"),
            UdfParameter.result(FieldSpec.DataType.STRING)
                .asMultiValued()
                .withDescription("Union of both arrays (distinct values)")
        ))
        .addExample("union with overlap", List.of("a", "b", "c"), List.of("c", "d", "e"),
            List.of("a", "b", "c", "d", "e"))
        .addExample("union with no overlap", List.of("a", "b"), List.of("c", "d"), List.of("a", "b", "c", "d"))
        .addExample("union with duplicates", List.of("a", "b", "b"), List.of("b", "c", "c"), List.of("a", "b", "c"))
        .addExample("union with empty array1", List.of(), List.of("a", "b"), List.of("a", "b"))
        .addExample("union with empty array2", List.of("a", "b"), List.of(), List.of("a", "b"))
        .addExample("union with both empty", List.of(), List.of(), List.of())
        .addExample(UdfExample.create("null array1", null, List.of("a", "b"), null)
            .withoutNull(List.of("a", "b")))
        .addExample(UdfExample.create("null array2", List.of("a", "b"), null, null)
            .withoutNull(List.of("a", "b")))
        .addExample(UdfExample.create("null both", null, null, null).withoutNull(List.of()))
        .build()
        .generateExamples();
  }
}
