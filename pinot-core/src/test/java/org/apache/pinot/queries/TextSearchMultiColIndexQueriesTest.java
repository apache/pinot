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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.Test;


/**
 * Functional tests for text search feature on multi-column text index.
 * Test is similar to TextSearchQueriesTest, but uses a single Multi-Column text index with same columns.
 */
public class TextSearchMultiColIndexQueriesTest extends TextSearchQueriesTest {

  @Override
  protected TableConfig initTableConfig() {
    return new TableConfigBuilder(TableType.OFFLINE)
        .setTableName(TABLE_NAME)
        .setNoDictionaryColumns(RAW_TEXT_INDEX_COLUMNS)
        .setInvertedIndexColumns(DICT_TEXT_INDEX_COLUMNS)
        .setFieldConfigList(createFieldConfigs())
        .setMultiColumnTextIndexConfig(getMultiColumnTextIndexConfig())
        .build();
  }

  private static MultiColumnTextIndexConfig getMultiColumnTextIndexConfig() {
    return new MultiColumnTextIndexConfig(
        List.of(
            QUERY_LOG_TEXT_COL_NAME, SKILLS_TEXT_COL_NAME, SKILLS_TEXT_COL_DICT_NAME,
            SKILLS_TEXT_COL_MULTI_TERM_NAME, SKILLS_TEXT_NO_RAW_NAME, SKILLS_TEXT_MV_COL_NAME,
            SKILLS_TEXT_MV_COL_DICT_NAME),
        Map.of(
            // Originally config was specific to SKILLS_TEXT_NO_RAW_NAME
            // It's disabled because it affects forward index, and it's not clear if really needed.
            /*FieldConfig.TEXT_INDEX_NO_RAW_DATA, "true",
            FieldConfig.TEXT_INDEX_RAW_VALUE, "ILoveCoding"*/
        ),
        Map.of(
            SKILLS_TEXT_COL_NAME,
            Map.of(FieldConfig.TEXT_INDEX_STOP_WORD_INCLUDE_KEY, "coordinator",
                FieldConfig.TEXT_INDEX_STOP_WORD_EXCLUDE_KEY, "it, those",
                FieldConfig.TEXT_INDEX_ENABLE_PREFIX_SUFFIX_PHRASE_QUERIES, "true"),
            SKILLS_TEXT_COL_MULTI_TERM_NAME, Map.of(FieldConfig.TEXT_INDEX_USE_AND_FOR_MULTI_TERM_QUERIES, "true"),
            SKILLS_TEXT_COL_DICT_NAME, Map.of(FieldConfig.TEXT_INDEX_STOP_WORD_EXCLUDE_KEY, "")
        ));
  }

  @Override
  protected List<FieldConfig> createFieldConfigs() {
    List<FieldConfig> configs = new ArrayList<>();
    configs.add(new FieldConfig(QUERY_LOG_TEXT_COL_NAME, FieldConfig.EncodingType.DICTIONARY,
        List.of(), null, null));

    configs.add(
        new FieldConfig(SKILLS_TEXT_COL_NAME, FieldConfig.EncodingType.DICTIONARY, List.of(),
            null, null));

    configs.add(new FieldConfig(SKILLS_TEXT_COL_DICT_NAME, FieldConfig.EncodingType.DICTIONARY,
        List.of(), null, null));

    configs.add(new FieldConfig(SKILLS_TEXT_COL_MULTI_TERM_NAME, FieldConfig.EncodingType.DICTIONARY,
        List.of(), null, null));

    configs.add(new FieldConfig(SKILLS_TEXT_NO_RAW_NAME, FieldConfig.EncodingType.DICTIONARY,
        List.of(), null, null));

    configs.add(new FieldConfig(SKILLS_TEXT_MV_COL_NAME, FieldConfig.EncodingType.DICTIONARY,
        List.of(), null, null));

    configs.add(new FieldConfig(SKILLS_TEXT_MV_COL_DICT_NAME, FieldConfig.EncodingType.DICTIONARY,
        List.of(), null, null));

    return configs;
  }

  @Test(enabled = false)
  public void testMultiThreadedLuceneRealtime() {
    // ignore because test is not specific to multi-col index and is pretty slow
  }

  // TODO: restore test case once config is supported (if it ever happens)
  @Override
  protected boolean queryDefault() {
    return false;
  }
}
