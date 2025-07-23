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
public class ArrayContainsStringUdf extends Udf.FromAnnotatedMethod {
  public ArrayContainsStringUdf() throws NoSuchMethodException {
    super(ArrayFunctions.class.getMethod("arrayContainsString", String[].class, String.class));
  }

  @Override
  public String getDescription() {
    return "Checks if a string array contains a given string value.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("array", FieldSpec.DataType.STRING)
                .asMultiValued()
                .withDescription("Array of strings to search in"),
            UdfParameter.of("value", FieldSpec.DataType.STRING)
                .withDescription("String value to search for"),
            UdfParameter.result(FieldSpec.DataType.BOOLEAN)
                .withDescription("True if value is found, false otherwise")
        ))
        .addExample("value present", List.of("a", "b", "c"), "b", true)
        .addExample("value absent", List.of("a", "b", "c"), "d", false)
        .addExample("empty array", List.of(), "a", false)
        .addExample(UdfExample.create("null array", null, "a", null).withoutNull(false))
        .addExample(UdfExample.create("null value", List.of("a", "b", "c"), null, null).withoutNull(false))
        .addExample(UdfExample.create("null input", null, null, null).withoutNull(false))
        .build()
        .generateExamples();
  }
}

