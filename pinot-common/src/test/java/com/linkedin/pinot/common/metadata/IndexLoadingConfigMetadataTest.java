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
package com.linkedin.pinot.common.metadata;

import com.linkedin.pinot.common.metadata.segment.IndexLoadingConfigMetadata;
import java.util.Arrays;
import java.util.Set;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;


public class IndexLoadingConfigMetadataTest {
  private final static String KEY_OF_LOADING_INVERTED_INDEX = "metadata.loading.inverted.index.columns";
  private final static String KEY_OF_ENABLE_DEFAULT_COLUMNS = "enable.default.columns";

  @Test
  public void testInvertedIndexConfig() {
    Configuration resourceMetadata = getTestResourceMetadata();
    IndexLoadingConfigMetadata indexLoadingConfigMetadata = new IndexLoadingConfigMetadata(resourceMetadata);
    Set<String> loadingInvertedIndexColumns = indexLoadingConfigMetadata.getLoadingInvertedIndexColumns();

//    System.out.println("loadingInvertedIndexColumns is " + Arrays.toString(loadingInvertedIndexColumns.toArray(new String[0])));
    Assert.assertEquals(10, loadingInvertedIndexColumns.size());
    for (int j = 0; j < 10; ++j) {
      String columnName = "col" + j;
      Assert.assertTrue(indexLoadingConfigMetadata.isLoadingInvertedIndexForColumn(columnName));
    }
    for (int j = 10; j < 20; ++j) {
      String columnName = "col" + j;
      Assert.assertFalse(indexLoadingConfigMetadata.isLoadingInvertedIndexForColumn(columnName));
    }
  }

  @Test
  public void testEnableDefaultColumnsConfig() {
    Configuration resourceMetadata = getTestResourceMetadata();
    IndexLoadingConfigMetadata indexLoadingConfigMetadata = new IndexLoadingConfigMetadata(resourceMetadata);
    Assert.assertTrue(indexLoadingConfigMetadata.isEnableDefaultColumns());
  }

  private Configuration getTestResourceMetadata() {
    Configuration resourceMetadata = new PropertiesConfiguration();
    String columnNames = null;
    for (int i = 0; i < 10; ++i) {
      if (columnNames == null) {
        columnNames = ("col" + i);
      } else {
        columnNames += (", col" + i);
      }
    }
    resourceMetadata.addProperty(KEY_OF_LOADING_INVERTED_INDEX, columnNames);
    resourceMetadata.addProperty(KEY_OF_ENABLE_DEFAULT_COLUMNS, true);
    return resourceMetadata;
  }
}
