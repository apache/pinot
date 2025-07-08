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
package org.apache.pinot.integration.tests;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.config.table.FieldConfig;


/**
 * Integration test for CLPV2 compression codec
 * Extends CLPEncodingRealtimeIntegrationTest to reuse all test logic
 */
public class CLPV2EncodingRealtimeIntegrationTest extends CLPEncodingRealtimeIntegrationTest {

  @Override
  protected List<FieldConfig> getFieldConfigs() {
    List<FieldConfig> fieldConfigs = new ArrayList<>();

    fieldConfigs.add(new FieldConfig.Builder("logLine").withEncodingType(FieldConfig.EncodingType.RAW)
        .withCompressionCodec(FieldConfig.CompressionCodec.CLPV2).build());

    return fieldConfigs;
  }
} 