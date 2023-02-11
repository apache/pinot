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
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
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
  public void testGetColumnPartitionMap() {
    Map<String, Set<Integer>> columnPartitionMap =
        QueryOptionsUtils.getColumnPartitionMap(ImmutableMap.of("columnPartitionMap", "k1:1/k1:2/k1:3/k2:4/k2:5/k2:6"));

    Assert.assertEquals(columnPartitionMap.get("k1"), ImmutableSet.of(1, 2, 3));
    Assert.assertEquals(columnPartitionMap.get("k2"), ImmutableSet.of(4, 5, 6));
  }
}
