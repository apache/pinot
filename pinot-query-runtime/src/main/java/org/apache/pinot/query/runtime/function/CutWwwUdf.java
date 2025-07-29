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
public class CutWwwUdf extends Udf.FromAnnotatedMethod {

  public CutWwwUdf() throws NoSuchMethodException {
    super(UrlFunctions.class.getMethod("cutWWW", String.class));
  }

  @Override
  public String getDescription() {
    return "Removes the leading 'www.' from the domain in the URL, if present "
        // TODO: This should be changed to return null if the input is not a valid URL.
        + "or the same input if the argument is not a valid url.";
  }

  @Override
  public Map<UdfSignature, Set<UdfExample>> getExamples() {
    return UdfExampleBuilder.forSignature(UdfSignature.of(
            UdfParameter.of("url", FieldSpec.DataType.STRING)
                .withDescription("The URL string."),
            UdfParameter.result(FieldSpec.DataType.STRING)
                .withDescription("The URL with 'www.' removed from the domain, or unchanged if not present.")
        ))
        .addExample("with www", "https://www.example.com", "https://example.com")
        .addExample("without www", "https://example.com", "https://example.com")
        .addExample(UdfExample.create("null input", null, null).withoutNull("null"))
        .addExample("invalid url", "invalid-url", "invalid-url")
        .build()
        .generateExamples();
  }
}

