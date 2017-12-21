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

import com.linkedin.pinot.common.data.StarTreeIndexSpec;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONException;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IndexingConfigTest {

  @Test
  public void testSerDe()
      throws JSONException, IOException {
    JSONObject json = new JSONObject();
    json.put("invertedIndexColumns", Arrays.asList("a", "b", "c"));
    json.put("sortedColumn", Arrays.asList("d", "e", "f"));

    String[] expectedOnHeapDictionaryColumns = new String[] {"x", "y", "z"};
    json.put("onHeapDictionaryColumns", Arrays.asList(expectedOnHeapDictionaryColumns));
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

    List<String> actualOnHeapDictionaryColumns = indexingConfig.getOnHeapDictionaryColumns();
    Assert.assertEquals(actualOnHeapDictionaryColumns.size(), expectedOnHeapDictionaryColumns.length);
    for (int i = 0; i < expectedOnHeapDictionaryColumns.length; i++) {
      Assert.assertEquals(actualOnHeapDictionaryColumns.get(i), expectedOnHeapDictionaryColumns[i]);
    }
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

  /**
   * Unit test to check get and set of star tree index spec on IndexingConfig.
   * <ul>
   *   <li> Creates a StarTreeIndexSpec and sets it into the IndexingConfig. </li>
   *   <li> Indexing config is first serialized into a string, and then read back from string. </li>
   *   <li> Test to ensure star tree index spec values are correct after serialization and de-serialization. </li>
   * </ul>
   * @throws IOException
   */
  @Test
  public void testStarTreeSpec()
      throws IOException {
    Random random = new Random(System.nanoTime());
    StarTreeIndexSpec expectedStarTreeSpec = new StarTreeIndexSpec();

    List<String> expectedDimensionSplitOrder = Arrays.asList("col1", "col2", "col3");
    expectedStarTreeSpec.setDimensionsSplitOrder(expectedDimensionSplitOrder);

    int expectedMaxLeafRecords = random.nextInt();
    expectedStarTreeSpec.setMaxLeafRecords(expectedMaxLeafRecords);

    int expectedSkipMaterializationThreshold = random.nextInt();
    expectedStarTreeSpec.setSkipMaterializationCardinalityThreshold(expectedSkipMaterializationThreshold);

    Set<String> expectedSkipMaterializationDimensions = new HashSet<>(Arrays.asList(new String[]{"col4", "col5"}));
    expectedStarTreeSpec.setSkipMaterializationForDimensions(expectedSkipMaterializationDimensions);

    Set<String> expectedSkipStarNodeCreationForDimension = new HashSet<>(Arrays.asList(new String[]{"col6", "col7"}));
    expectedStarTreeSpec.setSkipStarNodeCreationForDimensions(expectedSkipStarNodeCreationForDimension);

    IndexingConfig expectedIndexingConfig = new IndexingConfig();
    expectedIndexingConfig.setStarTreeIndexSpec(expectedStarTreeSpec);

    ObjectMapper objectMapper = new ObjectMapper();
    String indexingConfigString = objectMapper.writeValueAsString(expectedIndexingConfig);

    IndexingConfig actualIndexingConfig = objectMapper.readValue(indexingConfigString, IndexingConfig.class);
    StarTreeIndexSpec actualStarTreeSpec = actualIndexingConfig.getStarTreeIndexSpec();

    Assert.assertEquals(actualStarTreeSpec.getDimensionsSplitOrder(), expectedDimensionSplitOrder);
    Assert.assertEquals(actualStarTreeSpec.getMaxLeafRecords(), expectedMaxLeafRecords);

    Assert.assertEquals(actualStarTreeSpec.getSkipMaterializationCardinalityThreshold(),
        expectedSkipMaterializationThreshold);
    Assert.assertEquals(actualStarTreeSpec.getSkipMaterializationForDimensions(),
        expectedSkipMaterializationDimensions);
    Assert.assertEquals(actualStarTreeSpec.getSkipStarNodeCreationForDimensions(),
        expectedSkipStarNodeCreationForDimension);
  }
}
