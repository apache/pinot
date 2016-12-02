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

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.mutable.MutableLong;


/**
 * Performance test for lookup in string dictionary.
 */
public class StringDictionaryPerfTest {
  private static final int MAX_STRING_LENGTH = 100;
  private static final String TMP_DIR = System.getProperty("java.io.tmpdir");
  private static final String COLUMN_NAME = "test";
  private static final int TOTAL_NUM_LOOKUPS = 100_000;

  String[] _inputStrings;
  private File _indexDir;
  private int _dictLength;

  /**
   * Helper method to build a segment:
   * <ul>
   *   <li> Segment contains one string column </li>
   *   <li> Row values for the column are randomly generated strings of length 1 to 100 </li>
   * </ul>
   *
   * @param dictLength Length of the dictionary
   * @throws Exception
   */
  public void buildSegment(int dictLength)
      throws Exception {
    Schema schema = new Schema();
    String segmentName = "perfTestSegment" + System.currentTimeMillis();
    _indexDir = new File(TMP_DIR + File.separator + segmentName);
    _indexDir.deleteOnExit();

    FieldSpec fieldSpec = new DimensionFieldSpec(COLUMN_NAME, FieldSpec.DataType.STRING, true);
    schema.addField(fieldSpec);

    _dictLength = dictLength;
    _inputStrings = new String[dictLength];

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    config.setOutDir(_indexDir.getParent());
    config.setFormat(FileFormat.AVRO);
    config.setSegmentName(segmentName);

    Random random = new Random(System.nanoTime());
    final List<GenericRow> data = new ArrayList<>();
    Set<String> uniqueStrings = new HashSet<>(dictLength);

    int i = 0;
    while (i < dictLength) {
      HashMap<String, Object> map = new HashMap<>();
      String randomString = RandomStringUtils.randomAlphanumeric(1 + random.nextInt(MAX_STRING_LENGTH));

      if (uniqueStrings.contains(randomString)) {
        continue;
      }

      _inputStrings[i] = randomString;
      uniqueStrings.add(randomString);
      map.put("test", _inputStrings[i++]);

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      data.add(genericRow);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    RecordReader reader = getGenericRowRecordReader(schema, data);
    driver.init(config, reader);
    driver.build();
  }

  /**
   * Measures the performance of string dictionary lookups by performing the provided
   * number of lookups to random indices.
   *
   * @param numLookups Number of lookups to perform
   * @throws Exception
   */
  public void perfTestLookups(int numLookups)
      throws Exception {
    IndexSegmentImpl segment = (IndexSegmentImpl) Loaders.IndexSegment.load(_indexDir, ReadMode.heap);
    ImmutableDictionaryReader dictionary = segment.getDictionaryFor(COLUMN_NAME);

    Random random = new Random(System.nanoTime());
    long start = System.currentTimeMillis();

    for (int i = 0; i < numLookups; i++) {
      int index = 1 + random.nextInt(_dictLength);
      dictionary.indexOf(_inputStrings[index]);
    }

    FileUtils.deleteQuietly(_indexDir);
    System.out.println("Total time for " + TOTAL_NUM_LOOKUPS + " lookups: " + (System.currentTimeMillis() - start));
  }

  /**
   * Returns an implementation of GenericRow record reader.
   *
   * @param schema Schema for the data
   * @param data Data
   * @return GenericRow record reader
   */
  private static RecordReader getGenericRowRecordReader(final Schema schema, final List<GenericRow> data) {
    return new RecordReader() {
      int _counter = 0;

      @Override
      public void rewind()
          throws Exception {
        _counter = 0;
      }

      @Override
      public GenericRow next() {
        return data.get(_counter++);
      }

      @Override
      public GenericRow next(GenericRow row) {
        return next();
      }

      @Override
      public void init()
          throws Exception {
      }

      @Override
      public boolean hasNext() {
        return _counter < data.size();
      }

      @Override
      public Schema getSchema() {
        return schema;
      }

      @Override
      public Map<String, MutableLong> getNullCountMap() {
        return null;
      }

      @Override
      public void close()
          throws Exception {
      }
    };
  }

  public static void main(String[] args)
      throws Exception {
    if (args.length != 2) {
      System.out.println("Usage: StringDictionaryPerfRunner <dictionary_length> <num_lookups> ");
    }

    int dictLength = Integer.valueOf(args[0]);
    int numLookups = Integer.valueOf(args[1]);

    StringDictionaryPerfTest test = new StringDictionaryPerfTest();
    test.buildSegment(dictLength);
    test.perfTestLookups(numLookups);
  }
}
