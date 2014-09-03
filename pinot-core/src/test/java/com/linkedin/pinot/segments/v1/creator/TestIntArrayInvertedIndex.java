package com.linkedin.pinot.segments.v1.creator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.data.readers.RecordReaderFactory;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnMetadata;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegment;
import com.linkedin.pinot.core.indexsegment.columnar.ColumnarSegmentLoader;
import com.linkedin.pinot.core.indexsegment.columnar.IntArrayInvertedIndex;
import com.linkedin.pinot.core.indexsegment.columnar.IntArrayInvertedIndexLoader;
import com.linkedin.pinot.core.indexsegment.columnar.creator.ColumnarSegmentCreator;
import com.linkedin.pinot.core.indexsegment.columnar.creator.V1Constants;
import com.linkedin.pinot.core.indexsegment.creator.SegmentCreatorFactory;
import com.linkedin.pinot.core.indexsegment.dictionary.Dictionary;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfiguration;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.time.SegmentTimeUnit;


public class TestIntArrayInvertedIndex {
  private final String AVRO_DATA = "data/sample_data.avro";
  private static File INDEX_DIR = new File(TestIntArrayInvertedIndex.class.toString());

  @Test(enabled = false)
  public void test1() throws ConfigurationException, IOException {
    ColumnarSegment segment = (ColumnarSegment) ColumnarSegmentLoader.load(INDEX_DIR, ReadMode.heap);
    Map<String, Dictionary<?>> dictionaryMap = segment.getDictionaryMap();
    Map<String, ColumnMetadata> medataMap = segment.getColumnMetadataMap();
    Map<String, IntArrayInvertedIndex> invertedIndexMap = new HashMap<String, IntArrayInvertedIndex>();
    Map<String, IntArraysWrapper> rawInvertedIndexMap = new HashMap<String, IntArraysWrapper>();

    // load inverted indexes in mmap mode
    for (String column : dictionaryMap.keySet()) {
      File file = new File(INDEX_DIR, column + V1Constants.Indexes.INTARRAY_INVERTED_INDEX_FILE_EXTENSION);
      ColumnMetadata metadata = medataMap.get(column);
      IntArrayInvertedIndex invertedIndex = IntArrayInvertedIndexLoader.load(file, ReadMode.heap, metadata);
      invertedIndexMap.put(column, invertedIndex);
    }

    // compare the loaded inverted index with the record in avro file
    ColumnarSegment heapSegment = (ColumnarSegment) ColumnarSegmentLoader.load(INDEX_DIR, ReadMode.heap);
    // decompress inverted index to heap for quick testing
    for (String column : heapSegment.getColumnMetadataMap().keySet()) {
      int size = heapSegment.getDictionaryFor(column).size();
      IntArraysWrapper arraysWrapper = new IntArraysWrapper(size);
      rawInvertedIndexMap.put(column, arraysWrapper);
      for (int i = 0; i < size; ++i) {
        arraysWrapper.arrays[i] = invertedIndexMap.get(column).get(i);
      }
    }
    @SuppressWarnings("resource")
    DataFileStream<GenericRecord> reader =
        new DataFileStream<GenericRecord>(new FileInputStream(new File(getClass().getClassLoader()
            .getResource(AVRO_DATA).getFile())), new GenericDatumReader<GenericRecord>());
    int docId = 0;
    while (reader.hasNext()) {
      GenericRecord rec = reader.next();
      for (String column : heapSegment.getColumnMetadataMap().keySet()) {
        Object entry = rec.get(column);
        if (entry instanceof Utf8) {
          entry = ((Utf8) entry).toString();
        }
        int dicId = heapSegment.getDictionaryFor(column).indexOf(entry);
        // make sure that docId for dicId exist in the inverted index
        AssertJUnit.assertEquals(binarySearch(rawInvertedIndexMap.get(column).arrays[dicId], docId), true);
        int size = heapSegment.getDictionaryFor(column).size();
        for (int i = 0; i < size; ++i) {
          if (i == dicId) {
            continue;
          }
          // make sure that docId for dicId does not exist in the inverted index
          AssertJUnit.assertEquals(binarySearch(rawInvertedIndexMap.get(column).arrays[i], docId), false);
        }
      }
      ++docId;
    }
  }

  private static class IntArraysWrapper {
    public int[][] arrays;

    public IntArraysWrapper(int size) {
      this.arrays = new int[size][];
    }
  }

  private boolean binarySearch(int[] array, int x) {
    return Arrays.binarySearch(array, x) >= 0;
  }

  @AfterClass
  public void teardown() {
    FileUtils.deleteQuietly(INDEX_DIR);
  }

  @BeforeClass
  public void setup() throws Exception {
    String filePath = getClass().getClassLoader().getResource(AVRO_DATA).getFile();

    if (INDEX_DIR.exists()) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }

    SegmentGeneratorConfiguration config =
        SegmentTestUtils.getSegmentGenSpecWithSchemAndProjectedColumns(new File(filePath), INDEX_DIR, "daysSinceEpoch",
            SegmentTimeUnit.days, "test", "testTable");

    ColumnarSegmentCreator creator =
        (ColumnarSegmentCreator) SegmentCreatorFactory.get(SegmentVersion.v1, RecordReaderFactory.get(config));
    creator.init(config);
    creator.buildSegment();

    System.out.println("built at : " + INDEX_DIR.getAbsolutePath());
  }
}
