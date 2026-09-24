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
package org.apache.pinot.segment.local.indexsegment.immutable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.local.segment.index.column.PhysicalColumnIndexContainer;
import org.apache.pinot.segment.local.segment.index.readers.text.MultiColumnLuceneTextIndexReader;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;


public class ColumnMaterializerTest {

  @Test
  public void testCreateIndexContainerAttachesMultiColumnTextIndexToItsColumnsOnly() {
    // A reader without any index yields an empty container, enough to observe the attachment
    SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
    MultiColumnLuceneTextIndexReader multiColumnTextIndex = mock(MultiColumnLuceneTextIndexReader.class);
    ColumnMaterializer materializer =
        new ColumnMaterializer(reader, Map.of(), false, multiColumnTextIndex, Set.of("t"));

    PhysicalColumnIndexContainer t = (PhysicalColumnIndexContainer) materializer.createIndexContainer(metadata("t"));
    assertSame(t.getMultiColumnTextIndex(), multiColumnTextIndex);
    PhysicalColumnIndexContainer u = (PhysicalColumnIndexContainer) materializer.createIndexContainer(metadata("u"));
    assertNull(u.getMultiColumnTextIndex());
    assertNull(t.getIndex(StandardIndexes.forward()));
  }

  @Test
  public void testCreateIndexContainerWrapsReadFailure()
      throws IOException {
    SegmentDirectory.Reader reader = mock(SegmentDirectory.Reader.class);
    when(reader.hasIndexFor("a", StandardIndexes.forward())).thenReturn(true);
    when(reader.getIndexFor("a", StandardIndexes.forward())).thenThrow(new IOException("disk"));
    ColumnMaterializer materializer = new ColumnMaterializer(reader, Map.of(), true, null, Set.of());

    UncheckedIOException e =
        expectThrows(UncheckedIOException.class, () -> materializer.createIndexContainer(metadata("a")));
    assertTrue(e.getMessage().contains("a"), e.getMessage());
    assertEquals(e.getCause().getMessage(), "disk");
  }

  private static ColumnMetadataImpl metadata(String column) {
    return ColumnMetadataImpl.builder().setFieldSpec(new DimensionFieldSpec(column, FieldSpec.DataType.STRING, true))
        .setTotalDocs(10).setCardinality(10).setHasDictionary(false).build();
  }
}
