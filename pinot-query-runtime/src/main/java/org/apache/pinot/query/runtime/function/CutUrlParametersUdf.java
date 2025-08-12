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
public class CutUrlParametersUdf extends Udf.FromAnnotatedMethod {

  public CutUrlParametersUdf() throws NoSuchMethodException {
    super(UrlFunctions.class.getMethod("cutURLParameters", String.class, String[].class));
  }

  @Override
  public String getDescription() {
    return "Removes multiple query parameters from the URL "
        // TODO: This should be changed to return null if the input is not a valid URL.
        + "or the same input if the argument is not a valid url.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("url", FieldSpec.DataType.STRING)
                .withDescription("The URL string."),
            UdfParameter.of("names", FieldSpec.DataType.STRING)
                .asMultiValued()
                .withDescription("The parameter names to remove."),
            UdfParameter.result(FieldSpec.DataType.STRING)
                .withDescription("The URL with the parameters removed.")
        ))
        .addExample("remove params", "https://example.com/path?a=1&b=2&c=3", new String[]{"a","c"}, "https://example.com/path?b=2")
        .addExample("none present", "https://example.com/path?a=1", new String[]{"x","y"}, "https://example.com/path?a=1")
        .addExample(UdfExample.create("null input", null, new String[]{"a"}, null).withoutNull("null"))
        .addExample("invalid url", "invalid-url", new String[]{"a"}, "invalid-url")
        .build()
        .generateExamples();
  }
}

