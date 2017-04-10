/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.config;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IndexingConfigTest {

  @Test
  public void testIgnoreUnknown()
      throws JSONException, IOException {
    JSONObject json = new JSONObject();
    json.put("invertedIndexColumns", Arrays.asList("a", "b", "c"));
    json.put("sortedColumn", Arrays.asList("d", "e", "f"));
    json.put("loadMode", "MMAP");
    json.put("keyThatIsUnknown", "randomValue");

    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree(json.toString());
    IndexingConfig indexingConfig = mapper.readValue(jsonNode, IndexingConfig.class);

    Assert.assertEquals("MMAP", indexingConfig.getLoadMode());
    List<String> invertedIndexColumns = indexingConfig.getInvertedIndexColumns();
    Assert.assertEquals(3, invertedIndexColumns.size());
    Assert.assertEquals("a", invertedIndexColumns.get(0));
    Assert.assertEquals("b", invertedIndexColumns.get(1));
    Assert.assertEquals("c", invertedIndexColumns.get(2));

    List<String> sortedIndexColumns = indexingConfig.getSortedColumn();
    Assert.assertEquals(3, sortedIndexColumns.size());
    Assert.assertEquals("d", sortedIndexColumns.get(0));
    Assert.assertEquals("e", sortedIndexColumns.get(1));
    Assert.assertEquals("f", sortedIndexColumns.get(2));
  }

  @Test
  public void testSegmentPartitionConfig()
      throws IOException {
    int numColumns = 5;
    Map<String, ColumnPartitionConfig> expectedColumnPartitionMap = new HashMap<>(5);
    for (int i = 0; i < numColumns; i++) {
      expectedColumnPartitionMap.put("column_" + i, new ColumnPartitionConfig("function_" + i, i + 1));
    }

    SegmentPartitionConfig expectedPartitionConfig = new SegmentPartitionConfig(expectedColumnPartitionMap);
    IndexingConfig expectedIndexingConfig = new IndexingConfig();
    expectedIndexingConfig.setSegmentPartitionConfig(expectedPartitionConfig);

    ObjectMapper mapper = new ObjectMapper();
    String indexingConfigString = mapper.writeValueAsString(expectedIndexingConfig);
    IndexingConfig actualIndexingConfig = mapper.readValue(indexingConfigString, IndexingConfig.class);

    SegmentPartitionConfig actualPartitionConfig = actualIndexingConfig.getSegmentPartitionConfig();
    Map<String, ColumnPartitionConfig> actualColumnPartitionMap = actualPartitionConfig.getColumnPartitionMap();
    Assert.assertEquals(actualColumnPartitionMap.size(), expectedColumnPartitionMap.size());

    for (String column : expectedColumnPartitionMap.keySet()) {
      Assert.assertEquals(actualPartitionConfig.getFunctionName(column),
          expectedPartitionConfig.getFunctionName(column));
      Assert.assertEquals(actualPartitionConfig.getNumPartitions(column),
          expectedPartitionConfig.getNumPartitions(column));
    }
  }
}
