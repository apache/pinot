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
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.function.scalar.UrlFunctions;
import org.apache.pinot.core.udf.Udf;
import org.apache.pinot.core.udf.UdfExample;
import org.apache.pinot.core.udf.UdfExampleBuilder;
import org.apache.pinot.core.udf.UdfParameter;
import org.apache.pinot.core.udf.UdfSignature;
import org.apache.pinot.spi.data.FieldSpec;

@AutoService(Udf.class)
public class ExtractUrlParameterUdf extends Udf.FromAnnotatedMethod {

  public ExtractUrlParameterUdf() throws NoSuchMethodException {
    super(UrlFunctions.class.getMethod("extractURLParameter", String.class, String.class));
  }

  @Override
  public String getDescription() {
    return "Extracts the value of a specific query parameter from the URL.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("url", FieldSpec.DataType.STRING)
                .withDescription("The URL string."),
            UdfParameter.of("name", FieldSpec.DataType.STRING)
                .withDescription("The parameter name to extract."),
            UdfParameter.result(FieldSpec.DataType.STRING)
                .withDescription("The value of the parameter, or an empty string if not found or invalid.")
        ))
        .addExample("extract param", "https://example.com/path?a=1&b=2", "a", "1")
        .addExample("param not present", "https://example.com/path?a=1", "b", "")
        .addExample(UdfExample.create("null input", null, "a", null).withoutNull(""))
        .addExample(UdfExample.create("null name", "https://example.com/path?a=1", null, null).withoutNull(""))
        .addExample(UdfExample.create("invalid url", "invalid-url", "a", "").withoutNull(""))
        .build()
        .generateExamples();
  }
}

