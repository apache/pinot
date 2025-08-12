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
public class ChrUdf extends Udf.FromAnnotatedMethod {

  public ChrUdf() throws NoSuchMethodException {
    super(StringFunctions.class.getMethod("chr", int.class));
  }

  @Override
  public String getDescription() {
    return "Returns the character corresponding to the given Unicode code point.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("codePoint", FieldSpec.DataType.INT)
                .withDescription("The Unicode code point to convert to a character."),
            UdfParameter.result(FieldSpec.DataType.STRING)
                .withDescription("The character corresponding to the code point.")
        ))
        .addExample("chr(65)", 65, "A")
        .addExample("chr(97)", 97, "a")
        .addExample("chr(8364)", 8364, "€")
        .addExample(UdfExample.create("null input", null, null).withoutNull(""))
        .build()
        .generateExamples();
  }
}
