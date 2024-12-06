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
package org.apache.pinot.segment.local.recordtransformer.enricher.clp;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.auto.service.AutoService;
import java.io.IOException;
import org.apache.pinot.spi.recordtransformer.enricher.RecordEnricher;
import org.apache.pinot.spi.recordtransformer.enricher.RecordEnricherFactory;
import org.apache.pinot.spi.recordtransformer.enricher.RecordEnricherValidationConfig;
import org.apache.pinot.spi.utils.JsonUtils;


@AutoService(RecordEnricherFactory.class)
public class CLPEncodingEnricherFactory implements RecordEnricherFactory {
  private static final String ENRICHER_TYPE = "clpEnricher";

  @Override
  public String getEnricherType() {
    return ENRICHER_TYPE;
  }

  @Override
  public RecordEnricher createEnricher(JsonNode enricherProps)
      throws IOException {
    return new CLPEncodingEnricher(enricherProps);
  }

  @Override
  public void validateEnrichmentConfig(JsonNode enricherProps, RecordEnricherValidationConfig validationConfig) {
    try {
      JsonUtils.jsonNodeToObject(enricherProps, CLPEncodingEnricherConfig.class);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to parse clp enricher config", e);
    }
  }
}
