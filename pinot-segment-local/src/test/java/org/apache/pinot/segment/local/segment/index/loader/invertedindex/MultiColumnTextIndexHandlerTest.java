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
package org.apache.pinot.segment.local.segment.index.loader.invertedindex;

import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.spi.index.multicolumntext.MultiColumnTextMetadata;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.MultiColumnTextIndexConfig;
import org.testng.annotations.Test;

import static org.apache.pinot.segment.local.segment.index.loader.invertedindex.MultiColumnTextIndexHandler.shouldModifyMultiColTextIndex;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;


public class MultiColumnTextIndexHandlerTest {

  @Test
  public void testShouldModifyMultiColTextIndex() {
    assertFalse(shouldModifyMultiColTextIndex(null, null));

    assertFalse(shouldModifyMultiColTextIndex(
        new MultiColumnTextIndexConfig(List.of("colA", "colB"),
            null, null),
        new MultiColumnTextMetadata(MultiColumnTextMetadata.VERSION_1, List.of("colA", "colB"),
            null, null)
    ));

    assertFalse(shouldModifyMultiColTextIndex(
        new MultiColumnTextIndexConfig(List.of("colA", "colB"),
            null,
            Map.of("colA", Map.of(FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS, "that"))),
        new MultiColumnTextMetadata(MultiColumnTextMetadata.VERSION_1, List.of("colA", "colB"),
            null,
            Map.of("colA", Map.of(FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS, "that"))
        )));

    assertFalse(shouldModifyMultiColTextIndex(
        new MultiColumnTextIndexConfig(List.of("colA", "colB"),
            Map.of(FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS, "this"),
            null),
        new MultiColumnTextMetadata(MultiColumnTextMetadata.VERSION_1, List.of("colA", "colB"),
            Map.of(FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS, "this"),
            null)
    ));

    MultiColumnTextMetadata oldCfg =
        new MultiColumnTextMetadata(MultiColumnTextMetadata.VERSION_1, List.of("colA", "colB"),
            Map.of(FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS, "this"),
            Map.of("colA", Map.of(FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS, "that"))
        );

    MultiColumnTextIndexConfig newCfg = new MultiColumnTextIndexConfig(List.of("colA", "colB"),
        Map.of(FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS, "this"),
        Map.of("colA", Map.of(FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS, "that")));

    assertFalse(shouldModifyMultiColTextIndex(newCfg, oldCfg));

    assertTrue(shouldModifyMultiColTextIndex(newCfg, null));
    assertTrue(shouldModifyMultiColTextIndex(null, oldCfg));

    assertTrue(shouldModifyMultiColTextIndex(new MultiColumnTextIndexConfig(List.of("colA", "colB"),
            null, null),
        new MultiColumnTextMetadata(MultiColumnTextMetadata.VERSION_1, List.of("colA", "colB", "colC"),
            null, null)
    ));

    assertTrue(shouldModifyMultiColTextIndex(new MultiColumnTextIndexConfig(List.of("colA", "colB"),
            null, null),
        new MultiColumnTextMetadata(MultiColumnTextMetadata.VERSION_1, List.of("colA"),
            null, null)
    ));

    assertTrue(shouldModifyMultiColTextIndex(new MultiColumnTextIndexConfig(List.of("colA", "colB"),
            Map.of(FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS, "this"),
            null),
        new MultiColumnTextMetadata(MultiColumnTextMetadata.VERSION_1, List.of("colA", "colB"),
            Map.of(FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS, "CHANGED"),
            null)
    ));

    assertTrue(shouldModifyMultiColTextIndex(new MultiColumnTextIndexConfig(List.of("colA", "colB"),
            null, null),
        new MultiColumnTextMetadata(MultiColumnTextMetadata.VERSION_1, List.of("colA", "colB"),
            Map.of(FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS, "ADDED"),
            null)
    ));

    assertTrue(shouldModifyMultiColTextIndex(new MultiColumnTextIndexConfig(List.of("colA", "colB"),
            Map.of(FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS, "this"),
            null),
        new MultiColumnTextMetadata(MultiColumnTextMetadata.VERSION_1, List.of("colA", "colB"),
            null, // removed
            null)
    ));

    assertTrue(shouldModifyMultiColTextIndex(new MultiColumnTextIndexConfig(List.of("colA", "colB"),
            null, null),
        new MultiColumnTextMetadata(MultiColumnTextMetadata.VERSION_1, List.of("colA", "colB"),
            null,
            Map.of("colA", Map.of(FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS, "ADDED")))
    ));

    assertTrue(shouldModifyMultiColTextIndex(new MultiColumnTextIndexConfig(List.of("colA", "colB"),
            null,
            Map.of("colA", Map.of(FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS, "that"))),
        new MultiColumnTextMetadata(MultiColumnTextMetadata.VERSION_1, List.of("colA", "colB"),
            null,
            Map.of("colA", Map.of(FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS, "CHANGED")))
    ));

    assertTrue(shouldModifyMultiColTextIndex(new MultiColumnTextIndexConfig(List.of("colA", "colB"),
            null,
            Map.of("colA", Map.of(FieldConfig.TEXT_INDEX_LUCENE_QUERY_PARSER_CLASS, "that"))),
        new MultiColumnTextMetadata(MultiColumnTextMetadata.VERSION_1, List.of("colA", "colB"),
            null,
            null) // removed
    ));
  }

  @Test
  public void testShouldModifyMultiColTextIndexIgnoredUnknownColumnProps() {
    assertFalse(shouldModifyMultiColTextIndex(new MultiColumnTextIndexConfig(List.of("colA", "colB"),
            null, null),
        new MultiColumnTextMetadata(MultiColumnTextMetadata.VERSION_1, List.of("colA", "colB"),
            null,
            Map.of("colA", Map.of("bogusConfig", "new"))) // added
    ));

    assertFalse(shouldModifyMultiColTextIndex(new MultiColumnTextIndexConfig(List.of("colA", "colB"),
            null,
            Map.of("colA", Map.of("bogusConfig", "old"))),
        new MultiColumnTextMetadata(MultiColumnTextMetadata.VERSION_1, List.of("colA", "colB"),
            null,
            Map.of("colA", Map.of("bogusConfig", "new"))) // changed
    ));

    assertFalse(shouldModifyMultiColTextIndex(new MultiColumnTextIndexConfig(List.of("colA", "colB"),
            null,
            Map.of("colA", Map.of("bogusConfig", "old"))),
        new MultiColumnTextMetadata(MultiColumnTextMetadata.VERSION_1, List.of("colA", "colB"),
            null,
            null) // removed
    ));
  }
}
