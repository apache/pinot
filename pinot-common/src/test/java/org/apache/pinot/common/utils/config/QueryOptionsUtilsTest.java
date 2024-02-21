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

package org.apache.pinot.common.utils.config;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.utils.CommonConstants;
import org.testng.Assert;
import org.testng.annotations.Test;


public class QueryOptionsUtilsTest {

  @Test
  public void shouldConvertCaseInsensitiveMapToUseCorrectValues() {
    // Given:
    Map<String, String> configs = ImmutableMap.of(
        "ENABLENullHandling", "true",
        "useMULTISTAGEEngine", "false"
    );

    // When:
    Map<String, String> resolved = QueryOptionsUtils.resolveCaseInsensitiveOptions(configs);

    // Then:
    Assert.assertEquals(resolved.get(CommonConstants.Broker.Request.QueryOptionKey.ENABLE_NULL_HANDLING), "true");
    Assert.assertEquals(resolved.get(CommonConstants.Broker.Request.QueryOptionKey.USE_MULTISTAGE_ENGINE), "false");
  }

  @Test
  public void testIndexSkipConfigParsing() {
    String indexSkipConfigStr = "col1=inverted,range&col2=sorted";
    Map<String, String> queryOptions =
        Map.of(CommonConstants.Broker.Request.QueryOptionKey.INDEX_SKIP_CONFIG, indexSkipConfigStr);
    Map<String, Set<FieldConfig.IndexType>> indexSkipConfig = QueryOptionsUtils.getIndexSkipConfig(queryOptions);
    Assert.assertEquals(indexSkipConfig.get("col1"),
        Set.of(FieldConfig.IndexType.RANGE, FieldConfig.IndexType.INVERTED));
    Assert.assertEquals(indexSkipConfig.get("col2"),
        Set.of(FieldConfig.IndexType.SORTED));
  }
}
