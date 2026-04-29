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
package org.apache.pinot.segment.local.segment.index.range;

import java.util.Set;
import org.apache.pinot.segment.spi.index.DictionaryIndexConfig;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.RangeIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class RangeIndexTypeTest {

  /// Range index encodes values differently depending on whether the column is dictionary-encoded (range over
  /// dictionary IDs) or raw (range over the raw values), so the existing on-disk range index becomes invalid the
  /// moment the dictionary is added or removed for the column. The
  /// [IndexType#shouldInvalidateOnDictionaryChange] contract must surface this so the segment-reload path knows to
  /// drop and rebuild the range index.
  @Test
  public void rangeIndexMustBeInvalidatedWhenDictionaryStateChanges() {
    FieldSpec fieldSpec = new DimensionFieldSpec("col", FieldSpec.DataType.INT, true);
    assertTrue(StandardIndexes.range().shouldInvalidateOnDictionaryChange(fieldSpec, RangeIndexConfig.DEFAULT),
        "Range index on-disk format depends on dictionary presence and must be rebuilt on dictionary change");
  }

  @Test
  public void disabledRangeIndexIsNotInvalidatedByHelper() {
    FieldSpec fieldSpec = new DimensionFieldSpec("col", FieldSpec.DataType.INT, true);
    FieldIndexConfigs configs = new FieldIndexConfigs.Builder()
        .add(StandardIndexes.range(), RangeIndexConfig.DISABLED)
        .build();
    Set<IndexType<?, ?, ?>> toRebuild =
        DictionaryIndexConfig.getIndexTypesToInvalidateOnDictionaryChange(fieldSpec, configs);
    assertFalse(toRebuild.contains(StandardIndexes.range()),
        "A disabled range index must not appear in the rebuild list");
  }

  /**
   * End-to-end shape of the user-visible scenario:
   *
   * <ol>
   *   <li>A column starts with a raw forward index and a raw range index (no dictionary).</li>
   *   <li>The user enables a dictionary on that column.</li>
   *   <li>On segment reload, the dictionary handler builds the new dictionary, and the loader consults
   *       {@link DictionaryIndexConfig#getIndexTypesToInvalidateOnDictionaryChange} to find every index whose
   *       on-disk payload has been invalidated by the dictionary state change. The range index must be in that
   *       set so it is dropped and rebuilt against the new dictionary IDs.</li>
   * </ol>
   *
   * This test asserts step (3): the rebuild list, computed from the post-change config (range still enabled,
   * dictionary now enabled), includes the range index.
   */
  @Test
  public void rangeIndexIsScheduledForRebuildAfterDictionaryEnable() {
    FieldSpec fieldSpec = new DimensionFieldSpec("col", FieldSpec.DataType.INT, true);

    // Post-change config: range index enabled, dictionary now enabled too. The forward-index encoding is
    // intentionally not part of this contract — encoding reconciliation is owned by ForwardIndexConfig and
    // FieldIndexConfigsUtil; the rebuild-on-dictionary-change signal is what this test exercises.
    FieldIndexConfigs configs = new FieldIndexConfigs.Builder()
        .add(StandardIndexes.range(), RangeIndexConfig.DEFAULT)
        .add(StandardIndexes.dictionary(), new DictionaryIndexConfig(false, false))
        .build();

    Set<IndexType<?, ?, ?>> toRebuild =
        DictionaryIndexConfig.getIndexTypesToInvalidateOnDictionaryChange(fieldSpec, configs);

    assertTrue(toRebuild.contains(StandardIndexes.range()),
        "Range index must be marked for rebuild after a dictionary is added to the column");
  }
}
