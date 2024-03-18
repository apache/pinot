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
package org.apache.pinot.spi.recordenricher;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.pinot.spi.config.table.ingestion.EnrichmentConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RecordEnricherRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(RecordEnricherRegistry.class);
  private static final Map<String, RecordEnricherFactory> RECORD_ENRICHER_FACTORY_MAP = new HashMap<>();

  private RecordEnricherRegistry() {
  }

  public static void validateEnrichmentConfig(EnrichmentConfig enrichmentConfig,
      RecordEnricherValidationConfig config) {
    if (!RECORD_ENRICHER_FACTORY_MAP.containsKey(enrichmentConfig.getEnricherType())) {
      throw new IllegalArgumentException("No record enricher found for type: " + enrichmentConfig.getEnricherType());
    }

    RECORD_ENRICHER_FACTORY_MAP.get(enrichmentConfig.getEnricherType())
        .validateEnrichmentConfig(enrichmentConfig.getProperties(), config);
  }

  public static RecordEnricher createRecordEnricher(EnrichmentConfig enrichmentConfig)
      throws IOException {
    if (!RECORD_ENRICHER_FACTORY_MAP.containsKey(enrichmentConfig.getEnricherType())) {
      throw new IllegalArgumentException("No record enricher found for type: " + enrichmentConfig.getEnricherType());
    }
    return RECORD_ENRICHER_FACTORY_MAP.get(enrichmentConfig.getEnricherType())
        .createEnricher(enrichmentConfig.getProperties());
  }

  static {
    for (RecordEnricherFactory recordEnricherFactory : ServiceLoader.load(RecordEnricherFactory.class)) {
      LOGGER.info("Registered record enricher factory type: {}", recordEnricherFactory.getEnricherType());
      RECORD_ENRICHER_FACTORY_MAP.put(recordEnricherFactory.getEnricherType(), recordEnricherFactory);
    }
  }
}
