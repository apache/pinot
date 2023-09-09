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
package org.apache.pinot.spotbugs.plugin;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Collections;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.TimestampConfig;

/**
 * This is an invalid case where a constructor that requires a jackson argument is called
 */
class BadSpecialCase {
  void method()
      throws IOException {
    new FieldConfig("asd", FieldConfig.EncodingType.RAW, (FieldConfig.IndexType) null, Collections.emptyList(),
        FieldConfig.CompressionCodec.LZ4, (TimestampConfig) null, (JsonNode) null, Collections.emptyMap(),
        (JsonNode) null);
  }
}
