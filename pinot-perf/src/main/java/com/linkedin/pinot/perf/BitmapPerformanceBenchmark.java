/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.perf;

import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.metadata.column.ColumnMetadata;
import com.linkedin.pinot.core.metadata.segment.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.column.ColumnIndexContainer;
import com.linkedin.pinot.core.segment.index.readers.BitmapInvertedIndexReader;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import com.linkedin.pinot.core.segment.index.readers.IntDictionary;
import com.linkedin.pinot.core.segment.index.readers.InvertedIndexReader;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import com.linkedin.pinot.core.segment.store.SegmentDirectory;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.configuration.ConfigurationException;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class BitmapPerformanceBenchmark {

  public static void iterationSpeed(String indexSegmentDir, String column) throws Exception {
    File indexSegment = new File(indexSegmentDir);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexSegment);
    Map<String, BitmapInvertedIndexReader> bitMapIndexMap = new HashMap<String, BitmapInvertedIndexReader>();
    Map<String, Integer> cardinalityMap = new HashMap<String, Integer>();
    Map<String, ImmutableDictionaryReader> dictionaryMap = new HashMap<String, ImmutableDictionaryReader>();
    File bitMapIndexFile = new File(indexSegmentDir, column + ".bitmap.inv");
    ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
    int cardinality = columnMetadata.getCardinality();
    cardinalityMap.put(column, cardinality);

    PinotDataBuffer bitMapDataBuffer = PinotDataBuffer.fromFile(bitMapIndexFile, ReadMode.mmap, FileChannel.MapMode.READ_ONLY, "testing");
    BitmapInvertedIndexReader bitmapInvertedIndex = new BitmapInvertedIndexReader(bitMapDataBuffer, cardinality);
    File dictionaryFile = new File(indexSegmentDir + "/" + column + ".dict");
    SegmentDirectory segmentDirectory = SegmentDirectory.createFromLocalFS(indexSegment, segmentMetadata, ReadMode.mmap);
    SegmentDirectory.Reader segmentReader = segmentDirectory.createReader();
    ColumnIndexContainer container =
        ColumnIndexContainer.init(segmentReader, columnMetadata, null);
    ImmutableDictionaryReader dictionary = container.getDictionary();
    dictionaryMap.put(column, dictionary);
    // System.out.println(column + ":\t" + MemoryUtil.deepMemoryUsageOf(bitmapInvertedIndex));
    bitMapIndexMap.put(column, bitmapInvertedIndex);
    int dictId = dictionary.indexOf("na.us");
    ImmutableRoaringBitmap immutable = bitmapInvertedIndex.getImmutable(dictId);
    Iterator<Integer> iterator = immutable.iterator();
    int count = 0;
    long start = System.currentTimeMillis();
    while (iterator.hasNext()){
      iterator.next();
      count = count + 1;
    }
    long end = System.currentTimeMillis();
    System.out.println(" matched: " + count  + " Time to iterate:"+ (end-start));
    bitMapDataBuffer.close();
  }

  public static void andSpeed(String indexSegmentDir) {

  }

  public static void main(String[] args) throws Exception {
    File indexDir = new File("/home/kgopalak/pinot_perf/index_dir/scinPricing_OFFLINE");
    File[] segmentDirs = indexDir.listFiles();
    for(File indexSegmentDir:segmentDirs){
      iterationSpeed(indexSegmentDir.getAbsolutePath(), "dimension_geo");
    }
  }

  public static void benchmarkIntersetionAndUnion(String indexSegmentDir)
      throws ConfigurationException, IOException, Exception {
    File[] listFiles = new File(indexSegmentDir).listFiles();
    File indexDir = new File(indexSegmentDir);
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(indexDir);
    Map<String, BitmapInvertedIndexReader> bitMapIndexMap = new HashMap<String, BitmapInvertedIndexReader>();
    Map<String, Integer> cardinalityMap = new HashMap<String, Integer>();
    Map<String, ImmutableDictionaryReader> dictionaryMap = new HashMap<String, ImmutableDictionaryReader>();
    for (File file : listFiles) {
      if (!file.getName().endsWith("bitmap.inv")) {
        continue;
      }
      String column = file.getName().replaceAll(".bitmap.inv", "");
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      int cardinality = columnMetadata.getCardinality();
      cardinalityMap.put(column, cardinality);
      System.out.println(column + "\t\t\t" + cardinality + "  \t" + columnMetadata.getDataType());
      PinotDataBuffer bitmapDataBuffer = PinotDataBuffer.fromFile(file, ReadMode.mmap, FileChannel.MapMode.READ_ONLY, "testing");
      BitmapInvertedIndexReader bitmapInvertedIndex = new BitmapInvertedIndexReader(bitmapDataBuffer, cardinality);
      File dictionaryFile = new File(indexSegmentDir + "/" + column + ".dict");
      SegmentDirectory segmentDirectory = SegmentDirectory.createFromLocalFS(indexDir, segmentMetadata, ReadMode.mmap);
      SegmentDirectory.Reader segmentReader = segmentDirectory.createReader();
      ColumnIndexContainer container =
          ColumnIndexContainer.init(segmentReader, columnMetadata, null);
      ImmutableDictionaryReader dictionary = container.getDictionary();
      if (columnMetadata.getDataType() == DataType.INT) {
        System.out.println("BitmapPerformanceBenchmark.main()");
        assert dictionary instanceof IntDictionary;
      }
      dictionaryMap.put(column, dictionary);
      // System.out.println(column + ":\t" + MemoryUtil.deepMemoryUsageOf(bitmapInvertedIndex));
      bitMapIndexMap.put(column, bitmapInvertedIndex);
      bitmapDataBuffer.close();
    }

    List<String> dimensionNamesList = segmentMetadata.getSchema().getDimensionNames();

    Collections.shuffle(dimensionNamesList);
    int NUM_TEST = 100;
    final int MAX_DIMENSIONS_PER_DIMENSION = 1;
    int MAX_DIMENSIONS_IN_WHERE_CLAUSE = 3;
    Random random = new Random();
    for (int numDimensions = 1; numDimensions <= MAX_DIMENSIONS_IN_WHERE_CLAUSE; numDimensions++) {
      for (int numValuesPerDimension =
          1; numValuesPerDimension <= MAX_DIMENSIONS_PER_DIMENSION; numValuesPerDimension++) {
        int runCount = 0;
        while (runCount < NUM_TEST) {
          Collections.shuffle(dimensionNamesList);
          List<ImmutableRoaringBitmap> bitMaps = new ArrayList<ImmutableRoaringBitmap>();
          List<String> columnNameValuePairs = new ArrayList<String>();
          for (int i = 0; i < numDimensions; i++) {
            String columnName = dimensionNamesList.get(i);
            InvertedIndexReader bitmapInvertedIndex = bitMapIndexMap.get(columnName);
            for (int j = 0; j < numValuesPerDimension; j++) {
              int dictId = random.nextInt(cardinalityMap.get(columnName));
              String dictValue = dictionaryMap.get(columnName).getStringValue(dictId);
              columnNameValuePairs.add(columnName + ":" + dictValue);
              ImmutableRoaringBitmap immutable = bitmapInvertedIndex.getImmutable(dictId);
              bitMaps.add(immutable);

            }
          }
          System.out.println("START**********************************");
          int[] cardinality = new int[bitMaps.size()];
          int[] sizes = new int[bitMaps.size()];
          for (int i = 0; i < bitMaps.size(); i++) {
            ImmutableRoaringBitmap immutableRoaringBitmap = bitMaps.get(i);
            cardinality[i] = immutableRoaringBitmap.getCardinality();
            sizes[i] = immutableRoaringBitmap.getSizeInBytes();
          }
          System.out.println("\t#bitmaps:" + bitMaps.size());
          System.out.println("\tinput values:" + columnNameValuePairs);

          System.out.println("\tinput cardinality:" + Arrays.toString(cardinality));
          System.out.println("\tinput sizes:" + Arrays.toString(sizes));

          and(bitMaps);
          or(bitMaps);
          System.out.println("END**********************************");

          runCount = runCount + 1;
        }
      }
    }
  }

  private static ImmutableRoaringBitmap or(List<ImmutableRoaringBitmap> bitMaps) {
    if (bitMaps.size() == 1) {
      return bitMaps.get(0);
    }
    MutableRoaringBitmap answer = ImmutableRoaringBitmap.or(bitMaps.get(0), bitMaps.get(1));
    long start = System.currentTimeMillis();
    for (int i = 2; i < bitMaps.size(); i++) {
      answer.or(bitMaps.get(i));
    }

    long end = System.currentTimeMillis();

    bitMaps.get(0).getCardinality();
    System.out.println("OR operation Took " + (end - start));
    System.out.println("\toutput cardinality:" + answer.getCardinality());
    System.out.println("\toutout sizes:" + answer.getSizeInBytes());
    return answer;

  }

  private static ImmutableRoaringBitmap and(List<ImmutableRoaringBitmap> bitMaps) {
    if (bitMaps.size() == 1) {
      return bitMaps.get(0);
    }
    MutableRoaringBitmap answer = ImmutableRoaringBitmap.and(bitMaps.get(0), bitMaps.get(1));
    long start = System.currentTimeMillis();
    for (int i = 2; i < bitMaps.size(); i++) {
      answer.and(bitMaps.get(i));
    }

    long end = System.currentTimeMillis();
    int[] cardinality = new int[bitMaps.size()];
    int[] sizes = new int[bitMaps.size()];
    for (int i = 0; i < bitMaps.size(); i++) {
      ImmutableRoaringBitmap immutableRoaringBitmap = bitMaps.get(i);
      cardinality[i] = immutableRoaringBitmap.getCardinality();
      sizes[i] = immutableRoaringBitmap.getSizeInBytes();
    }
    bitMaps.get(0).getCardinality();
    System.out.println("AND operation Took " + (end - start));
    System.out.println("\toutput cardinality:" + answer.getCardinality());
    System.out.println("\toutout sizes:" + answer.getSizeInBytes());
    return answer;

  }
}
