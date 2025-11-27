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
package org.apache.pinot.queries;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.table.FieldConfig;

/**
 * Test class that runs all the same tests as TextSearchQueriesTest but with storeInSegmentFile=true.
 * This ensures that all text search functionality works correctly with the store in segment file configuration.
 */
public class TextSearchQueriesWithCombinedFilesTest extends TextSearchQueriesTest {

  @Override
  protected List<FieldConfig> createFieldConfigs() {
    List<FieldConfig> fieldConfigs = super.createFieldConfigs();

    // Override all text index configurations to use storeInSegmentFile=true
    for (int i = 0; i < fieldConfigs.size(); i++) {
      FieldConfig fieldConfig = fieldConfigs.get(i);
      if (fieldConfig.getIndexTypes().contains(FieldConfig.IndexType.TEXT)) {
        Map<String, String> properties = new HashMap<>();
        if (fieldConfig.getProperties() != null) {
          properties.putAll(fieldConfig.getProperties());
        }
        properties.put("storeInSegmentFile", "true");

        fieldConfigs.set(i, new FieldConfig(
            fieldConfig.getName(),
            fieldConfig.getEncodingType(),
            fieldConfig.getIndexTypes(),
            fieldConfig.getCompressionCodec(),
            properties
        ));
      }
    }

    return fieldConfigs;
  }
}
