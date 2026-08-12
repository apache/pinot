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
package org.apache.pinot.segment.local.recordtransformer.enricher.function;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.service.AutoService;
import java.io.IOException;
import org.apache.pinot.common.evaluator.FunctionEvaluatorFactory;
import org.apache.pinot.spi.recordtransformer.enricher.RecordEnricher;
import org.apache.pinot.spi.recordtransformer.enricher.RecordEnricherCreationContext;
import org.apache.pinot.spi.recordtransformer.enricher.RecordEnricherFactory;
import org.apache.pinot.spi.recordtransformer.enricher.RecordEnricherValidationConfig;
import org.apache.pinot.spi.utils.JsonUtils;


@AutoService(RecordEnricherFactory.class)
public class CustomFunctionEnricherFactory implements RecordEnricherFactory {
  private static final String TYPE = "generateColumn";

  @Override
  public String getEnricherType() {
    return TYPE;
  }

  @Override
  public RecordEnricher createEnricher(JsonNode enricherProps)
      throws IOException {
    return new CustomFunctionEnricher(enricherProps);
  }

  @Override
  public RecordEnricher createEnricher(JsonNode enricherProps, RecordEnricherCreationContext creationContext)
      throws IOException {
    return new CustomFunctionEnricher(enricherProps, creationContext.getIngestionGroovyPolicy());
  }

  @Override
  public void validateSecurityPolicy(JsonNode enricherProps, RecordEnricherValidationConfig validationConfig) {
    if (enricherProps == null) {
      return;
    }
    JsonNode fieldToFunctionMap = enricherProps.get("fieldToFunctionMap");
    if (fieldToFunctionMap == null || !fieldToFunctionMap.isObject()) {
      return;
    }
    for (JsonNode function : fieldToFunctionMap) {
      if (function.isTextual()) {
        FunctionEvaluatorFactory.validateIngestionGroovyPolicy(function.textValue(),
            validationConfig.isGroovyDisabled());
      }
    }
  }

  @Override
  public void validateEnrichmentConfig(JsonNode enricherProps, RecordEnricherValidationConfig validationConfig) {
    CustomFunctionEnricherConfig config;
    try {
      config = JsonUtils.jsonNodeToObject(enricherProps, CustomFunctionEnricherConfig.class);
      for (String function : config.getFieldToFunctionMap().values()) {
        FunctionEvaluatorFactory.validateIngestionGroovyPolicy(function, validationConfig.isGroovyDisabled());
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to parse custom function enricher config", e);
    }
  }
}
