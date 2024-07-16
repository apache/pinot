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
package org.apache.pinot.perf;

import com.google.common.base.Joiner;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.FileFormat;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;


/**
 * Performance test for lookup in string dictionary.
 */
public class StringDictionaryPerfTest {
  private static final int MAX_STRING_LENGTH = 1000;
  private static final boolean USE_FIXED_SIZE_STRING = true;
  private static final String TMP_DIR = System.getProperty("java.io.tmpdir");
  private static final String COLUMN_NAME = "test";
  private static final String[] STATS_HEADERS = new String[]{
      "DictSize", "TimeTaken(ms)", "SegmentSize", "NumLookups", "Min", "Max", "Mean", "StdDev", "Median", "Skewness",
      "Kurtosis", "Variance", "BufferSize"
  };
  private static final Joiner COMMA_JOINER = Joiner.on(",");

  private final DescriptiveStatistics _statistics = new DescriptiveStatistics();
  private String[] _inputStrings;
  private File _indexDir;
  private int _dictLength;

  /**
   * Helper method to build a segment:
   * <ul>
   *   <li>Segment contains one string column</li>
   *   <li>Row values for the column are randomly generated strings of length 1 to 100</li>
   * </ul>
   */
  private void buildSegment(int dictLength)
      throws Exception {
    Schema schema = new Schema();
    String segmentName = "perfTestSegment" + System.currentTimeMillis();
    _indexDir = new File(TMP_DIR + File.separator + segmentName);
    _indexDir.deleteOnExit();

    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.STRING, true);
    schema.addField(fieldSpec);
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("test").build();

    _dictLength = dictLength;
    _inputStrings = new String[dictLength];

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setOutDir(_indexDir.getParent());
    config.setFormat(FileFormat.AVRO);
    config.setSegmentName(segmentName);

    Random random = new Random(System.nanoTime());
    List<GenericRow> rows = new ArrayList<>(dictLength);
    Set<String> uniqueStrings = new HashSet<>(dictLength);

    int i = 0;
    while (i < dictLength) {
      HashMap<String, Object> map = new HashMap<>();
      String randomString = RandomStringUtils
          .randomAlphanumeric(USE_FIXED_SIZE_STRING ? MAX_STRING_LENGTH : (1 + random.nextInt(MAX_STRING_LENGTH)));

      if (uniqueStrings.contains(randomString)) {
        continue;
      }

      _inputStrings[i] = randomString;
      if (uniqueStrings.add(randomString)) {
        _statistics.addValue(randomString.length());
      }
      map.put("test", _inputStrings[i++]);

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
    }

    long start = System.currentTimeMillis();
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();
    System.out.println("Total time for building segment: " + (System.currentTimeMillis() - start));
  }

  /**
   * Measures the performance of string dictionary lookups by performing the provided number of lookups to random value.
   */
  public void perfTestLookups(int numLookups)
      throws Exception {
    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(_indexDir, ReadMode.heap);
    Dictionary dictionary = immutableSegment.getDictionary(COLUMN_NAME);

    Random random = new Random(System.nanoTime());
    long start = System.currentTimeMillis();

    for (int i = 0; i < numLookups; i++) {
      dictionary.indexOf(_inputStrings[random.nextInt(_dictLength)]);
    }

    FileUtils.deleteQuietly(_indexDir);
    System.out.println("Total time for " + numLookups + " lookups: " + (System.currentTimeMillis() - start) + "ms");
  }

  /**
   * Measures the performance of string dictionary reads by performing the provided number of reads for random index.
   */
  private String[] perfTestGetValues(int numGetValues)
      throws Exception {
    Runtime r = Runtime.getRuntime();
    System.gc();
    long oldMemory = r.totalMemory() - r.freeMemory();
    IndexLoadingConfig defaultIndexLoadingConfig = new IndexLoadingConfig();
    defaultIndexLoadingConfig.setReadMode(ReadMode.heap);
    Set<String> columnNames = new HashSet<>();
    columnNames.add(COLUMN_NAME);
    defaultIndexLoadingConfig.setOnHeapDictionaryColumns(columnNames);

    ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(_indexDir, defaultIndexLoadingConfig);
    Dictionary dictionary = immutableSegment.getDictionary(COLUMN_NAME);

    Random random = new Random(System.nanoTime());
    long start = System.currentTimeMillis();
    for (int i = 0; i < numGetValues; i++) {
      dictionary.get(random.nextInt(_dictLength));
    }
    long time = System.currentTimeMillis() - start;

    System.gc();
    long newMemory = r.totalMemory() - r.freeMemory();
    long segmentSize = immutableSegment.getSegmentSizeBytes();
    FileUtils.deleteQuietly(_indexDir);

    System.out.println("Total time for " + numGetValues + " lookups: " + time + "ms");
    System.out.println("Memory usage: " + (newMemory - oldMemory));
    return new String[]{
        String.valueOf(_statistics.getN()), String.valueOf(time), String.valueOf(segmentSize),
        String.valueOf(numGetValues), String.valueOf(_statistics.getMin()), String.valueOf(_statistics.getMax()),
        String.valueOf(_statistics.getMean()), String.valueOf(_statistics.getStandardDeviation()),
        String.valueOf(_statistics.getPercentile(50.0D)), String.valueOf(_statistics.getSkewness()),
        String.valueOf(_statistics.getKurtosis()), String.valueOf(_statistics.getVariance())
    };
  }

  public static void main(String[] args)
      throws Exception {
    if (args.length < 2) {
      System.out.println("Usage: StringDictionaryPerfTest <dictionary_length> <dictionary_length> ... <num_lookups> ");
    }

    int numLookups = Integer.valueOf(args[args.length - 1]);

    String[][] stats = new String[args.length][];
    stats[0] = STATS_HEADERS;
    for (int i = 0; i < args.length - 1; i++) {
      int dictLength = Integer.valueOf(args[i]);
      StringDictionaryPerfTest test = new StringDictionaryPerfTest();
      test.buildSegment(dictLength);
      test.perfTestLookups(numLookups);
      stats[i + 1] = test.perfTestGetValues(numLookups);
    }
    for (String[] s : stats) {
      System.out.println(COMMA_JOINER.join(s));
    }
  }
}
