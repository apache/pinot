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

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;


public class TextIndexHandlerTest {
  private static final String COLUMN_NAME = "strCol";

  @DataProvider(name = "segmentVersions")
  public static Object[][] segmentVersions() {
    return new Object[][]{{SegmentVersion.v1}, {SegmentVersion.v3}};
  }

  /// A segment served from a [SegmentDirectory] that is not backed by a local directory has no index dir. The
  /// handler must not probe the file system for legacy native text index sidecar files in that case: those files
  /// only exist inside an on-disk segment directory, so the answer is simply that there are none.
  @Test(dataProvider = "segmentVersions")
  public void testNeedUpdateIndicesWithoutLocalIndexDir(SegmentVersion segmentVersion) {
    SegmentMetadataImpl segmentMetadata = mock(SegmentMetadataImpl.class);
    when(segmentMetadata.getName()).thenReturn("testSegment");
    when(segmentMetadata.getTotalDocs()).thenReturn(10);
    when(segmentMetadata.getAllColumns()).thenReturn(new TreeSet<>(Set.of(COLUMN_NAME)));
    when(segmentMetadata.getVersion()).thenReturn(segmentVersion);
    // No local directory backing this segment
    when(segmentMetadata.getIndexDir()).thenReturn(null);

    SegmentDirectory segmentDirectory = mock(SegmentDirectory.class);
    when(segmentDirectory.getSegmentMetadata()).thenReturn(segmentMetadata);
    when(segmentDirectory.getColumnsWithIndex(StandardIndexes.text())).thenReturn(Set.of());

    SegmentDirectory.Reader segmentReader = mock(SegmentDirectory.Reader.class);
    when(segmentReader.toSegmentDirectory()).thenReturn(segmentDirectory);

    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("testTable").build();
    Schema schema =
        new Schema.SchemaBuilder().addSingleValueDimension(COLUMN_NAME, DataType.STRING).build();
    TextIndexHandler handler = new TextIndexHandler(segmentDirectory, Map.of(), tableConfig, schema);

    // No text index is configured and none exists, so there is nothing to update
    assertFalse(handler.needUpdateIndices(segmentReader));
  }
}
