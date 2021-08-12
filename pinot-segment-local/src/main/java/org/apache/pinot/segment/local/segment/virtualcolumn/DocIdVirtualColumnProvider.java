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
package org.apache.pinot.segment.local.segment.virtualcolumn;

import java.io.IOException;
import org.apache.pinot.segment.local.segment.index.column.BaseVirtualColumnProvider;
import org.apache.pinot.segment.local.segment.index.readers.DocIdDictionary;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReaderContext;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.SortedIndexReader;
import org.apache.pinot.spi.utils.Pairs;


/**
 * Virtual column provider that returns the document id.
 */
public class DocIdVirtualColumnProvider extends BaseVirtualColumnProvider {
  private static final DocIdSortedIndexReader DOC_ID_SORTED_INDEX_READER = new DocIdSortedIndexReader();

  @Override
  public ForwardIndexReader<?> buildForwardIndex(VirtualColumnContext context) {
    return DOC_ID_SORTED_INDEX_READER;
  }

  @Override
  public Dictionary buildDictionary(VirtualColumnContext context) {
    return new DocIdDictionary(context.getTotalDocCount());
  }

  @Override
  public ColumnMetadataImpl buildMetadata(VirtualColumnContext context) {
    ColumnMetadataImpl.Builder columnMetadataBuilder = super.getColumnMetadataBuilder(context);
    columnMetadataBuilder.setCardinality(context.getTotalDocCount()).setSorted(true).setHasDictionary(true);
    return columnMetadataBuilder.build();
  }

  @Override
  public InvertedIndexReader<?> buildInvertedIndex(VirtualColumnContext context) {
    return DOC_ID_SORTED_INDEX_READER;
  }

  private static class DocIdSortedIndexReader implements SortedIndexReader<ForwardIndexReaderContext> {

    @Override
    public int getDictId(int docId, ForwardIndexReaderContext context) {
      return docId;
    }

    @Override
    public void readDictIds(int[] docIds, int length, int[] dictIdBuffer, ForwardIndexReaderContext context) {
      System.arraycopy(docIds, 0, dictIdBuffer, 0, length);
    }

    @Override
    public Pairs.IntPair getDocIds(int dictId) {
      return new Pairs.IntPair(dictId, dictId);
    }

    @Override
    public void close()
        throws IOException {
    }
  }
}
