package com.linkedin.pinot.core.chunk.index.loader;

import java.io.File;
import java.io.IOException;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.chunk.index.ChunkColumnMetadata;
import com.linkedin.pinot.core.chunk.index.ColumnarChunk;
import com.linkedin.pinot.core.chunk.index.readers.AbstractDictionaryReader;
import com.linkedin.pinot.core.chunk.index.readers.DoubleDictionary;
import com.linkedin.pinot.core.chunk.index.readers.FixedBitCompressedMVForwardIndexReader;
import com.linkedin.pinot.core.chunk.index.readers.FixedBitCompressedSVForwardIndexReader;
import com.linkedin.pinot.core.chunk.index.readers.FloatDictionary;
import com.linkedin.pinot.core.chunk.index.readers.IntDictionary;
import com.linkedin.pinot.core.chunk.index.readers.LongDictionary;
import com.linkedin.pinot.core.chunk.index.readers.StringDictionary;
import com.linkedin.pinot.core.index.reader.DataFileReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.columnar.BitmapInvertedIndex;


/**
 * @author Dhaval Patel<dpatel@linkedin.com>
 * Nov 13, 2014
 */

public class Loaders {

  public static IndexSegment load(File indexDir, ReadMode mode) throws Exception {
    return new ColumnarChunk(indexDir, mode);
  }

  public static class ForwardIndex {
    public static DataFileReader loadFwdIndexForColumn(ChunkColumnMetadata columnMetadata, File indexFile, ReadMode loadMode)
        throws Exception {
      DataFileReader fwdIndexReader;
      if (columnMetadata.isSingleValue()) {
        fwdIndexReader =
            new FixedBitCompressedSVForwardIndexReader(indexFile, columnMetadata.getTotalDocs(), columnMetadata.getBitsPerElement(),
                loadMode == ReadMode.mmap);
      } else {
        fwdIndexReader =
            new FixedBitCompressedMVForwardIndexReader(indexFile, columnMetadata.getTotalDocs(), columnMetadata.getBitsPerElement(),
                loadMode == ReadMode.mmap);
      }

      return fwdIndexReader;
    }
  }

  public static class InvertedIndex {
    public static BitmapInvertedIndex load(ChunkColumnMetadata metadata, File invertedIndexFile, ReadMode loadMode) throws IOException {
      return new BitmapInvertedIndex(invertedIndexFile, metadata.getCardinality(), false);
    }
  }

  public static class Dictionary {

    @SuppressWarnings("incomplete-switch")
    public static AbstractDictionaryReader load(ChunkColumnMetadata metadata, File dictionaryFile, ReadMode loadMode) throws IOException {
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
