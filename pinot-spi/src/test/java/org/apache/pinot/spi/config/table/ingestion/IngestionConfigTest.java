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
package org.apache.pinot.spi.config.table.ingestion;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class IngestionConfigTest {

  @Test
  public void deserializesConvertAggregationSourceTypes()
      throws JsonProcessingException {
    IngestionConfig config =
        JsonUtils.stringToObject("{\"convertAggregationSourceTypes\": true}", IngestionConfig.class);
    assertTrue(config.isConvertAggregationSourceTypes());
  }

  @Test
  public void convertAggregationSourceTypesDefaultsToFalse()
      throws JsonProcessingException {
    IngestionConfig config = JsonUtils.stringToObject("{}", IngestionConfig.class);
    assertFalse(config.isConvertAggregationSourceTypes());
  }

  @Test
  public void roundTripsConvertAggregationSourceTypesThroughJson()
      throws JsonProcessingException {
    IngestionConfig config = new IngestionConfig();
    config.setConvertAggregationSourceTypes(true);
    IngestionConfig deserialized = JsonUtils.stringToObject(config.toJsonString(), IngestionConfig.class);
    assertEquals(deserialized, config);
    assertTrue(deserialized.isConvertAggregationSourceTypes());
  }
}
