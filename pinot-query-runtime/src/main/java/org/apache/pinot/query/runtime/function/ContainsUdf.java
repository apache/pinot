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
import org.apache.pinot.common.function.scalar.StringFunctions;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;

@AutoService(Udf.class)
public class ContainsUdf extends Udf.FromAnnotatedMethod {

  public ContainsUdf() throws NoSuchMethodException {
    super(StringFunctions.class.getMethod("contains", String.class, String.class));
  }

  @Override
  public String getDescription() {
    return "Returns true if the input string contains the specified substring.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("input", FieldSpec.DataType.STRING)
                .withDescription("The input string."),
            UdfParameter.of("substring", FieldSpec.DataType.STRING)
                .withDescription("The substring to search for."),
            UdfParameter.result(FieldSpec.DataType.BOOLEAN)
                .withDescription("True if the input contains the substring, false otherwise.")
        ))
        .addExample("positive", "pinot", "no", true)
        .addExample("negative", "pinot", "abc", false)
        .addExample(UdfExample.create("null input", null, null, null).withoutNull(false))
        .addExample(UdfExample.create("null substring", "pinot", null, null).withoutNull(false))
        .addExample(UdfExample.create("both null", null, null, null).withoutNull(true))
        .build()
        .generateExamples();
  }
}
