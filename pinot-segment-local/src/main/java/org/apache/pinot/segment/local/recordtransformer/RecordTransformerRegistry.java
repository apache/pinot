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
package org.apache.pinot.segment.local.recordtransformer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RecordTransformerRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(RecordTransformerRegistry.class);
  private static final Map<String, RecordTransformerFactory> RECORD_ENRICHER_FACTORY_MAP = new HashMap<>();

  private RecordTransformerRegistry() {
  }

  public static void validateTransformConfig(TransformConfig transformConfig,
      RecordTransformerValidationConfig config) {
    if (!RECORD_ENRICHER_FACTORY_MAP.containsKey(transformConfig.getEnricherType())) {
      throw new IllegalArgumentException("No record enricher found for type: " + transformConfig.getEnricherType());
    }

    RECORD_ENRICHER_FACTORY_MAP.get(transformConfig.getEnricherType())
        .validateTransformConfig(transformConfig.getProperties(), config);
  }

  public static RecordTransformer createRecordTransformer(TransformConfig transformConfig)
      throws IOException {
    if (!RECORD_ENRICHER_FACTORY_MAP.containsKey(transformConfig.getEnricherType())) {
      throw new IllegalArgumentException("No record transformer found for type: " + transformConfig.getEnricherType());
    }
    return RECORD_ENRICHER_FACTORY_MAP.get(transformConfig.getEnricherType())
        .createTransformer(transformConfig.getProperties());
  }

  static {
    for (RecordTransformerFactory recordTransformerFactory : ServiceLoader.load(RecordTransformerFactory.class)) {
      LOGGER.info("Registered record enricher factory type: {}", recordTransformerFactory.getTransformerType());
      RECORD_ENRICHER_FACTORY_MAP.put(recordTransformerFactory.getTransformerType(), recordTransformerFactory);
    }
  }
}
