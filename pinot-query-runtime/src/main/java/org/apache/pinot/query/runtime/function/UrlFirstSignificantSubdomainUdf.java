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
public class UrlFirstSignificantSubdomainUdf extends Udf.FromAnnotatedMethod {

  public UrlFirstSignificantSubdomainUdf() throws NoSuchMethodException {
    super(UrlFunctions.class.getMethod("urlFirstSignificantSubdomain", String.class));
  }

  @Override
  public String getDescription() {
    return "Extracts the first significant subdomain from the URL.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("url", FieldSpec.DataType.STRING)
                .withDescription("The URL string."),
            UdfParameter.result(FieldSpec.DataType.STRING)
                .withDescription("The first significant subdomain, or null if invalid.")
        ))
        .addExample("normal domain", "https://a.b.example.com", "example")
        .addExample("short domain", "https://example.com", "example")
        .addExample(UdfExample.create("null input", null, null).withoutNull(""))
        .addExample(UdfExample.create("invalid url", "invalid-url", null).withoutNull(""))
        .build()
        .generateExamples();
  }
}

