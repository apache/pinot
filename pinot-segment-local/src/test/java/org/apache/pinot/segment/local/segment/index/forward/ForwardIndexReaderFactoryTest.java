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
package org.apache.pinot.segment.local.segment.index.forward;

import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;


public class ForwardIndexReaderFactoryTest {

  @Test
  public void testCodecSpecIsRejectedWhenPreprocessingIsSkipped() {
    ForwardIndexConfig config = new ForwardIndexConfig.Builder(FieldConfig.EncodingType.RAW)
        .withCodecSpec("LZ4")
        .build();
    FieldIndexConfigs fieldIndexConfigs = new FieldIndexConfigs.Builder()
        .add(StandardIndexes.forward(), config)
        .build();
    SegmentDirectory.Reader segmentReader = Mockito.mock(SegmentDirectory.Reader.class);
    ColumnMetadata metadata = Mockito.mock(ColumnMetadata.class);
    Mockito.when(metadata.getColumnName()).thenReturn("testCol");

    IllegalStateException exception = expectThrows(IllegalStateException.class,
        () -> ForwardIndexReaderFactory.getInstance()
            .createIndexReader(segmentReader, fieldIndexConfigs, metadata));
    assertEquals(exception.getMessage(), "codecSpec is not supported yet for column: testCol");
    Mockito.verifyNoInteractions(segmentReader);
  }
}
