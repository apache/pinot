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
public class ArrayIndexOfStringUdf extends Udf.FromAnnotatedMethod {
  public ArrayIndexOfStringUdf() throws NoSuchMethodException {
    super(ArrayFunctions.class.getMethod("arrayIndexOfString", String[].class, String.class));
  }

  @Override
  public String getDescription() {
    return "Returns the 1-based index of the first occurrence of the specified value in an array of strings. "
        + "If the value is not found, returns 0. If any argument is null, returns null.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("array", FieldSpec.DataType.STRING)
                .asMultiValued()
                .withDescription("Array of strings"),
            UdfParameter.of("value", FieldSpec.DataType.STRING)
                .withDescription("Value to search for"),
            UdfParameter.result(FieldSpec.DataType.INT)
                .withDescription("1-based index of the value in the array. "
                    + "If not found, returns 0. If any argument is null, returns null.")
        ))
        .addExample("single match", List.of("a", "b", "c"), "b", 2)
        .addExample("multiple matches", List.of("a", "b", "b", "c"), "b", 2)
        .addExample("no match", List.of("a", "b", "c"), "d", 0)
        .addExample("empty array", List.of(), "a", 0)
        .addExample(UdfExample.create("null array", null, "a", null))
        .addExample(UdfExample.create("null value", List.of("a", "b"), null, null))
        .addExample(UdfExample.create("null input", null, null, null))
        .build()
        .generateExamples();
  }
}
