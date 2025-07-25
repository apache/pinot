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
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.function.scalar.StringFunctions;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;

@AutoService(Udf.class)
public class CodepointUdf extends Udf.FromAnnotatedMethod {

  public CodepointUdf() throws NoSuchMethodException {
    super(StringFunctions.class.getMethod("codepoint", String.class));
  }

  @Override
  public String getDescription() {
    return "Returns the Unicode code point of the first character in the input string.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("input", FieldSpec.DataType.STRING)
                .withDescription("The input string."),
            UdfParameter.result(FieldSpec.DataType.INT)
                .withDescription("The Unicode code point of the first character.")
        ))
        .addExample("single character", "A", 65)
        .addExample("not ascii", "€", 8364)
        .addExample("a word", "pinot", 112)
        .addExample(UdfExample.create("null input", null, null).withoutNull(0))
        .build()
        .generateExamples();
  }
}

