/*
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
import com.google.common.collect.Lists;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.PinotScalarFunction;
import org.apache.pinot.common.function.scalar.ArrayFunctions;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;

@AutoService(Udf.class)
public class ArrayToStringUdf extends Udf {
  @Override
  public String getMainName() {
    return "arrayToString";
  }

  @Override
  public String getDescription() {
    return "Converts an array of strings into a single string, with elements joined by a delimiter.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("arr", FieldSpec.DataType.STRING)
                .asMultiValued()
                .withDescription("Input array of strings"),
            UdfParameter.of("delimiter", FieldSpec.DataType.STRING)
                .withDescription("Delimiter to join the array elements"),
            UdfParameter.result(FieldSpec.DataType.STRING)
                .withDescription("Joined string")
        ))
        .addExample("normal array", List.of("a", "b", "c"), ",", "a,b,c")
        .addExample("empty array", List.of(), ",", "")
        .addExample("single element", List.of("x"), ",", "x")
        .addExample("other separator", List.of("a", "b"), "/", "a/b")
        .addExample(UdfExample.create("null array", null, ",", null).withoutNull(""))
        .addExample(UdfExample.create("null delimeter", List.of("a", "b"), null, null).withoutNull("ab"))
        .and(
            UdfExampleBuilder.forSignature(
                UdfSignature.of(
                    UdfParameter.of("arr", FieldSpec.DataType.STRING)
                        .asMultiValued()
                        .withDescription("Input array of strings"),
                    UdfParameter.of("delimiter", FieldSpec.DataType.STRING)
                        .withDescription("Delimiter to join the array elements"),
                    UdfParameter.of("nullReplacement", FieldSpec.DataType.STRING)
                        .withDescription("Replacement for null elements or empty strings in the array."),
                    UdfParameter.result(FieldSpec.DataType.STRING)
                        .withDescription("Joined string with null replacements")
                ))
                .addExample("with nulls", Lists.newArrayList("a", null, "c", null), ",", "repl", "a,c")
                .addExample("empty replacement", List.of("a", "", "c"), ",", "repl", "a,repl,c")
                .addExample(UdfExample.create("null array", null, ",", "", null).withoutNull(""))
                .build()
            )
        .generateExamples();
  }

  @Override
  public PinotScalarFunction getScalarFunction() {
    try {
      Method twoArgMethod = ArrayFunctions.class.getMethod("arrayToString", String[].class, String.class);
      Method threeArgMethod =
          ArrayFunctions.class.getMethod("arrayToString", String[].class, String.class, String.class);

      return new FunctionRegistry.ArgumentCountBasedScalarFunction(
          getMainCanonicalName(),
          Map.of(
              2, FunctionInfo.fromMethod(twoArgMethod),
              3, FunctionInfo.fromMethod(threeArgMethod)
          )
      );
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }
}
