package com.linkedin.pinot.core.indexsegment.columnar;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.core.indexsegment.columnar.SegmentLoader.IO_MODE;
import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.heap.InMemoryDoubleDictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.heap.InMemoryFloatDictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.heap.InMemoryIntDictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.heap.InMemoryLongDictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.heap.InMemoryStringDictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.mmap.MmapDoubleDictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.mmap.MmapFloatDictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.mmap.MmapIntDictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.mmap.MmapLongDictionary;
import com.linkedin.pinot.core.indexsegment.dictionary.mmap.MmapStringDictionary;


public class DictionaryLoader {
  private static Logger logger = LoggerFactory.getLogger(DictionaryLoader.class);

  public static Dictionary<?> load(IO_MODE mode, File file, ColumnMetadata metadata) throws IOException {
    switch (mode) {
      case heap:
        return loadHeap(file, metadata);
      default:
        return loadMmap(file, metadata);
    }
  }

  public static Dictionary<?> loadMmap(File file, ColumnMetadata metadata) throws IOException {
    switch (metadata.getDataType()) {
      case INT:
        return new MmapIntDictionary(file, metadata.getDictionarySize());
      case LONG:
        return new MmapLongDictionary(file, metadata.getDictionarySize());
      case FLOAT:
        return new MmapFloatDictionary(file, metadata.getDictionarySize());
      case DOUBLE:
        return new MmapDoubleDictionary(file, metadata.getDictionarySize());
      default:
        return new MmapStringDictionary(file, metadata.getDictionarySize(),
            metadata.getPerElementSizeForStringDictionary());
    }
  }

  public static Dictionary<?> loadHeap(File file, ColumnMetadata metadata) throws IOException {
    switch (metadata.getDataType()) {
      case INT:
        return new InMemoryIntDictionary(file, metadata.getDictionarySize());
      case LONG:
        return new InMemoryLongDictionary(file, metadata.getDictionarySize());
      case FLOAT:
        return new InMemoryFloatDictionary(file, metadata.getDictionarySize());
      case DOUBLE:
        return new InMemoryDoubleDictionary(file, metadata.getDictionarySize());
      default:
        return new InMemoryStringDictionary(file, metadata.getDictionarySize(),
            metadata.getPerElementSizeForStringDictionary());
    }
  }
}
