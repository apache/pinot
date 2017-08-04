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
package com.linkedin.pinot.perf;

import com.google.common.collect.Lists;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.indexsegment.generator.SegmentVersion;
import com.linkedin.pinot.core.io.reader.BaseSingleColumnMultiValueReader;
import com.linkedin.pinot.core.io.reader.BaseSingleColumnSingleValueReader;
import com.linkedin.pinot.core.io.reader.impl.v1.MultiValueReaderContext;
import com.linkedin.pinot.core.segment.index.ColumnMetadata;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.memory.PinotDataBuffer;
import java.io.File;
import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import me.lemire.integercompression.BitPacking;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;


/**
 * Given a pinot segment directory, it benchmarks forward index scan speed
 */
public class ForwardIndexReaderBenchmark {
  static int MAX_RUNS = 10;

  public static void singleValuedReadBenchMarkV1(File file, int numDocs, int columnSizeInBits)
      throws Exception {
    boolean signed = false;
    boolean isMmap = false;
    PinotDataBuffer heapBuffer = PinotDataBuffer.fromFile(file, ReadMode.heap, FileChannel.MapMode.READ_ONLY, "benchmark");
    BaseSingleColumnSingleValueReader reader =
        new com.linkedin.pinot.core.io.reader.impl.v1.FixedBitSingleValueReader(heapBuffer, numDocs,
            columnSizeInBits, signed);
    // sequential read
    long start, end;
    DescriptiveStatistics stats = new DescriptiveStatistics();
    for (int run = 0; run < MAX_RUNS; run++) {
      start = System.currentTimeMillis();
      for (int i = 0; i < numDocs; i++) {
        int value = reader.getInt(i);
      }
      end = System.currentTimeMillis();
      stats.addValue(end - start);
    }
    System.out.println(" v1 sequential read stats for " + file.getName());
    System.out.println(
        stats.toString().replaceAll("\n", ", ") + " raw:" + Arrays.toString(stats.getValues()));
    reader.close();
    heapBuffer.close();
  }

  public static void singleValuedReadBenchMarkV2(File file, int numDocs, int numBits)
      throws Exception {
    boolean signed = false;
    boolean isMmap = false;
    long start, end;
    boolean fullScan = true;

    boolean batchRead = true;
    boolean singleRead = true;

    PinotDataBuffer heapBuffer = PinotDataBuffer.fromFile(file, ReadMode.heap, FileChannel.MapMode.READ_ONLY, "benchmarking");
    com.linkedin.pinot.core.io.reader.impl.v2.FixedBitSingleValueReader reader =
        new com.linkedin.pinot.core.io.reader.impl.v2.FixedBitSingleValueReader(heapBuffer, numDocs,
            numBits, signed);

    if (fullScan) {
      DescriptiveStatistics stats = new DescriptiveStatistics();
      ByteBuffer buffer = ByteBuffer.allocateDirect((int) file.length());
      RandomAccessFile raf = new RandomAccessFile(file, "r");
      raf.getChannel().read(buffer);
      raf.close();
      int[] input = new int[numBits];
      int[] output = new int[32];
      int numBatches = (numDocs + 31) / 32;
      for (int run = 0; run < MAX_RUNS; run++) {
        start = System.currentTimeMillis();
        for (int i = 0; i < numBatches; i++) {
          for (int j = 0; j < numBits; j++) {
            input[j] = buffer.getInt(i * numBits * 4 + j * 4);
          }
          BitPacking.fastunpack(input, 0, output, 0, numBits);
        }
        end = System.currentTimeMillis();
        stats.addValue((end - start));
      }
      System.out.println(" v2 full scan stats for " + file.getName());
      System.out.println(
          stats.toString().replaceAll("\n", ", ") + " raw:" + Arrays.toString(stats.getValues()));
    }
    if (singleRead) {
      DescriptiveStatistics stats = new DescriptiveStatistics();
      // sequential read
      for (int run = 0; run < MAX_RUNS; run++) {
        start = System.currentTimeMillis();
        for (int i = 0; i < numDocs; i++) {
          int value = reader.getInt(i);
        }
        end = System.currentTimeMillis();
        stats.addValue((end - start));
      }
      System.out.println(" v2 sequential single read for " + file.getName());
      System.out.println(
          stats.toString().replaceAll("\n", ", ") + " raw:" + Arrays.toString(stats.getValues()));
    }
    if (batchRead) {
      DescriptiveStatistics stats = new DescriptiveStatistics();
      int batchSize = Math.min(5000, numDocs);
      int[] output = new int[batchSize];
      int[] rowIds = new int[batchSize];

      // sequential read
      for (int run = 0; run < MAX_RUNS; run++) {
        start = System.currentTimeMillis();
        int rowId = 0;
        while (rowId < numDocs) {
          int length = Math.min(batchSize, numDocs - rowId);
          for (int i = 0; i < length; i++) {
            rowIds[i] = rowId + i;
          }
          reader.getIntBatch(rowIds, output, length);
          rowId = rowId + length;
        }
        end = System.currentTimeMillis();
        stats.addValue((end - start));
      }
      System.out.println("v2 sequential batch read stats for " + file.getName());
      System.out.println(
          stats.toString().replaceAll("\n", ", ") + " raw:" + Arrays.toString(stats.getValues()));
    }
    reader.close();

  }

  public static void multiValuedReadBenchMarkV1(File file, int numDocs, int totalNumValues,
      int maxEntriesPerDoc, int columnSizeInBits) throws Exception {
    System.out.println("******************************************************************");
    System.out.println(
        "Analyzing " + file.getName() + " numDocs:" + numDocs + ", totalNumValues:" + totalNumValues
            + ", maxEntriesPerDoc:" + maxEntriesPerDoc + ", numBits:" + columnSizeInBits);
    long start, end;
    boolean readFile = true;
    boolean randomRead = true;
    boolean contextualRead = true;
    boolean signed = false;
    boolean isMmap = false;
    PinotDataBuffer heapBuffer = PinotDataBuffer.fromFile(file, ReadMode.mmap, FileChannel.MapMode.READ_ONLY, "benchmarking");
    BaseSingleColumnMultiValueReader reader =
        new com.linkedin.pinot.core.io.reader.impl.v1.FixedBitMultiValueReader(heapBuffer, numDocs,
            totalNumValues, columnSizeInBits, signed);

    int[] intArray = new int[maxEntriesPerDoc];
    File outfile = new File("/tmp/" + file.getName() + ".raw");
    FileWriter fw = new FileWriter(outfile);
    for (int i = 0; i < numDocs; i++) {
      int length = reader.getIntArray(i, intArray);
      StringBuilder sb = new StringBuilder();
      String delim = "";
      for (int j = 0; j < length; j++) {
        sb.append(delim);
        sb.append(intArray[j]);
        delim = ",";
      }
      fw.write(sb.toString());
      fw.write("\n");
    }
    fw.close();

    // sequential read
    if (readFile) {
      DescriptiveStatistics stats = new DescriptiveStatistics();
      RandomAccessFile raf = new RandomAccessFile(file, "rw");
      ByteBuffer buffer = ByteBuffer.allocateDirect((int) file.length());
      raf.getChannel().read(buffer);
      for (int run = 0; run < MAX_RUNS; run++) {
        long length = file.length();
        start = System.currentTimeMillis();
        for (int i = 0; i < length; i++) {
          byte b = buffer.get(i);
        }
        end = System.currentTimeMillis();
        stats.addValue((end - start));
      }
      System.out.println("v1 multi value read bytes stats for " + file.getName());
      System.out.println(
          stats.toString().replaceAll("\n", ", ") + " raw:" + Arrays.toString(stats.getValues()));

      raf.close();
    }
    if (randomRead) {
      DescriptiveStatistics stats = new DescriptiveStatistics();
      for (int run = 0; run < MAX_RUNS; run++) {
        start = System.currentTimeMillis();
        for (int i = 0; i < numDocs; i++) {
          int length = reader.getIntArray(i, intArray);
        }
        end = System.currentTimeMillis();
        stats.addValue((end - start));
      }
      System.out.println("v1 multi value sequential read one stats for " + file.getName());
      System.out.println(
          stats.toString().replaceAll("\n", ", ") + " raw:" + Arrays.toString(stats.getValues()));
    }

    if (contextualRead) {
      DescriptiveStatistics stats = new DescriptiveStatistics();
      for (int run = 0; run < MAX_RUNS; run++) {
        MultiValueReaderContext context = (MultiValueReaderContext) reader.createContext();
        start = System.currentTimeMillis();
        for (int i = 0; i < numDocs; i++) {
          int length = reader.getIntArray(i, intArray, context);
        }
        end = System.currentTimeMillis();
        // System.out.println("RUN:" + run + "Time:" + (end-start));
        stats.addValue((end - start));
      }
      System.out
          .println("v1 multi value sequential read one with context stats for " + file.getName());
      System.out.println(
          stats.toString().replaceAll("\n", ", ") + " raw:" + Arrays.toString(stats.getValues()));

    }
    reader.close();
    heapBuffer.close();
    System.out.println("******************************************************************");

  }

  public static void multiValuedReadBenchMarkV2(File file, int numDocs, int totalNumValues,
      int maxEntriesPerDoc, int columnSizeInBits) throws Exception {
    boolean signed = false;
    boolean isMmap = false;
    boolean readOneEachTime = true;
    PinotDataBuffer heapBuffer = PinotDataBuffer.fromFile(file, ReadMode.heap, FileChannel.MapMode.READ_ONLY, "benchmarking");
    com.linkedin.pinot.core.io.reader.impl.v2.FixedBitMultiValueReader reader =
        new com.linkedin.pinot.core.io.reader.impl.v2.FixedBitMultiValueReader(heapBuffer, numDocs,
            totalNumValues, columnSizeInBits, signed);

    int[] intArray = new int[maxEntriesPerDoc];
    long start, end;

    // read one entry at a time
    if (readOneEachTime) {
      DescriptiveStatistics stats = new DescriptiveStatistics();

      for (int run = 0; run < MAX_RUNS; run++) {
        start = System.currentTimeMillis();
        for (int i = 0; i < numDocs; i++) {
          int length = reader.getIntArray(i, intArray);
        }
        end = System.currentTimeMillis();
        stats.addValue((end - start));
      }
      System.out.println("v2 multi value sequential read one stats for " + file.getName());
      System.out.println(
          stats.toString().replaceAll("\n", ", ") + " raw:" + Arrays.toString(stats.getValues()));
    }
    reader.close();
    heapBuffer.close();
  }

  private static void benchmarkForwardIndex(String indexDir) throws Exception {
    benchmarkForwardIndex(indexDir, null);
  }

  private static void benchmarkForwardIndex(String indexDir, List<String> includeColumns)
      throws Exception {
    SegmentMetadataImpl segmentMetadata = new SegmentMetadataImpl(new File(indexDir));
    String segmentVersion = segmentMetadata.getVersion();
    Set<String> columns = segmentMetadata.getAllColumns();
    for (String column : columns) {
      if (includeColumns != null && !includeColumns.isEmpty()) {
        if (!includeColumns.contains(column)) {
          continue;
        }
      }

      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata.isSingleValue()) {
        continue;
      }
      if (!columnMetadata.isSingleValue()) {

        String fwdIndexFileName = segmentMetadata.getForwardIndexFileName(column);
        File fwdIndexFile = new File(indexDir, fwdIndexFileName);
        multiValuedReadBenchMark(segmentVersion, fwdIndexFile, segmentMetadata.getTotalDocs(),
            columnMetadata.getTotalNumberOfEntries(), columnMetadata.getMaxNumberOfMultiValues(),
            columnMetadata.getBitsPerElement());
      } else if (columnMetadata.isSingleValue() && !columnMetadata.isSorted()) {
        String fwdIndexFileName = segmentMetadata.getForwardIndexFileName(column);
        File fwdIndexFile = new File(indexDir, fwdIndexFileName);
        singleValuedReadBenchMark(segmentVersion, fwdIndexFile, segmentMetadata.getTotalDocs(),
            columnMetadata.getBitsPerElement());
      }
    }
  }

  private static void multiValuedReadBenchMark(String segmentVersion, File fwdIndexFile,
      int totalDocs, int totalNumberOfEntries, int maxNumberOfMultiValues, int bitsPerElement)
          throws Exception {
    if (SegmentVersion.v1.name().equals(segmentVersion)) {
      multiValuedReadBenchMarkV1(fwdIndexFile, totalDocs, totalNumberOfEntries,
          maxNumberOfMultiValues, bitsPerElement);
    } else if (SegmentVersion.v2.name().equals(segmentVersion)) {
      multiValuedReadBenchMarkV2(fwdIndexFile, totalDocs, totalNumberOfEntries,
          maxNumberOfMultiValues, bitsPerElement);
    }
  }

  private static void singleValuedReadBenchMark(String segmentVersion, File fwdIndexFile,
      int totalDocs, int bitsPerElement) throws Exception {
    if (SegmentVersion.v1.name().equals(segmentVersion)) {
      singleValuedReadBenchMarkV1(fwdIndexFile, totalDocs, bitsPerElement);
    } else if (SegmentVersion.v2.name().equals(segmentVersion)) {
      singleValuedReadBenchMarkV2(fwdIndexFile, totalDocs, bitsPerElement);
    }
  }

  /**
   * USAGE ForwardIndexReaderBenchmark <indexDir> <comma delimited column_names(optional)>
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    String indexDir = args[0];
    if (args.length == 1) {
      benchmarkForwardIndex(indexDir);
    }
    if (args.length == 2) {
      benchmarkForwardIndex(indexDir, Lists.newArrayList(args[1].trim().split(",")));
    }
  }

}
