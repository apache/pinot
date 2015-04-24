/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.segment.index.loader;

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.index.reader.DataFileReader;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.BitmapInvertedIndexReader;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.InvertedIndexReader;
import com.linkedin.pinot.core.segment.index.SortedInvertedIndexReader;
import com.linkedin.pinot.core.segment.index.readers.DoubleDictionary;
import com.linkedin.pinot.core.segment.index.readers.FixedBitCompressedMVForwardIndexReader;
import com.linkedin.pinot.core.segment.index.readers.FixedBitCompressedSVForwardIndexReader;
import com.linkedin.pinot.core.segment.index.readers.FloatDictionary;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import com.linkedin.pinot.core.segment.index.readers.IntDictionary;
import com.linkedin.pinot.core.segment.index.readers.LongDictionary;
import com.linkedin.pinot.core.segment.index.readers.StringDictionary;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 13, 2014
 */

public class Loaders {

  public static class IndexSegment {
    public static com.linkedin.pinot.core.indexsegment.IndexSegment load(File indexDir, ReadMode mode) throws Exception {
      return new IndexSegmentImpl(indexDir, mode);
    }
  }

  public static class ForwardIndex {
    public static DataFileReader loadFwdIndexForColumn(ColumnMetadata columnMetadata, File indexFile, ReadMode loadMode)
        throws Exception {
      DataFileReader fwdIndexReader;
      if (columnMetadata.isSingleValue()) {
        fwdIndexReader =
            new FixedBitCompressedSVForwardIndexReader(indexFile, columnMetadata.getTotalDocs(),
                columnMetadata.getBitsPerElement(), loadMode == ReadMode.mmap, columnMetadata.hasNulls());
      } else {
        fwdIndexReader =
            new FixedBitCompressedMVForwardIndexReader(indexFile, columnMetadata.getTotalDocs(),
                columnMetadata.getBitsPerElement(), loadMode == ReadMode.mmap);
      }

      return fwdIndexReader;
    }
  }

  public static class InvertedIndex {
    public static InvertedIndexReader load(ColumnMetadata metadata, File indexDir, String column, ReadMode loadMode)
        throws IOException {
      if (metadata.isSorted()) {
        return new SortedInvertedIndexReader(new File(indexDir, column
            + V1Constants.Indexes.SORTED_INVERTED_INDEX_FILE_EXTENSION), metadata.getCardinality(),
            loadMode == ReadMode.mmap);
      }
      return new BitmapInvertedIndexReader(new File(indexDir, column
          + V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION), metadata.getCardinality(),
          loadMode == ReadMode.mmap);
    }
  }

  public static class Dictionary {

    @SuppressWarnings("incomplete-switch")
    public static ImmutableDictionaryReader load(ColumnMetadata metadata, File dictionaryFile, ReadMode loadMode)
        throws IOException {
      switch (metadata.getDataType()) {
        case INT:
          return new IntDictionary(dictionaryFile, metadata, loadMode);
        case LONG:
          return new LongDictionary(dictionaryFile, metadata, loadMode);
        case FLOAT:
          return new FloatDictionary(dictionaryFile, metadata, loadMode);
        case DOUBLE:
          return new DoubleDictionary(dictionaryFile, metadata, loadMode);
        case STRING:
        case BOOLEAN:
          return new StringDictionary(dictionaryFile, metadata, loadMode);
      }

      throw new UnsupportedOperationException("unsupported data type : " + metadata.getDataType());
    }
  }

}
