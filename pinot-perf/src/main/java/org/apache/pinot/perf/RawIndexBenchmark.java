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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.operator.DocIdSetOperator;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.docvalsets.ProjectionBlockValSet;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.TestFilterOperator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import picocli.CommandLine;


/**
 * Class to perform benchmark on lookups for dictionary encoded fwd index v.s. raw index without dictionary.
 * It can take an existing segment with two columns to compare. It can also create a segment on the fly with a
 * given input file containing strings (one string per line).
 */
@SuppressWarnings({"FieldCanBeLocal", "unused"})
@CommandLine.Command
public class RawIndexBenchmark {
  private static final String SEGMENT_DIR_NAME = System.getProperty("java.io.tmpdir") + File.separator + "rawIndexPerf";
  private static final String SEGMENT_NAME = "perfTestSegment";
  private static final int NUM_COLUMNS = 2;

  private static final String DEFAULT_RAW_INDEX_COLUMN = "column_0";
  private static final String DEFAULT_FWD_INDEX_COLUMN = "column_1";
  private static final int DEFAULT_NUM_LOOKUP = 100_000;
  private static final int DEFAULT_NUM_CONSECUTIVE_LOOKUP = 50;

  @CommandLine.Option(names = {"-segmentDir"}, required = false, description = "Untarred segment")
  private String _segmentDir = null;

  @CommandLine.Option(names = {"-fwdIndexColumn"}, required = false,
      description = "Name of column with dictionary encoded index")
  private String _fwdIndexColumn = DEFAULT_FWD_INDEX_COLUMN;

  @CommandLine.Option(names = {"-rawIndexColumn"}, required = false,
      description = "Name of column with raw index (no-dictionary")
  private String _rawIndexColumn = DEFAULT_RAW_INDEX_COLUMN;

  @CommandLine.Option(names = {"-dataFile"}, required = false,
      description = "File containing input data (one string per line)")
  private String _dataFile = null;

  @CommandLine.Option(names = {"-loadMode"}, required = false, description = "Load mode for data (mmap|heap")
  private String _loadMode = "heap";

  @CommandLine.Option(names = {"-numLookups"}, required = false,
      description = "Number of lookups to be performed for benchmark")
  private int _numLookups = DEFAULT_NUM_LOOKUP;

  @CommandLine.Option(names = {"-numConsecutiveLookups"}, required = false,
      description = "Number of consecutive docIds to lookup")
  private int _numConsecutiveLookups = DEFAULT_NUM_CONSECUTIVE_LOOKUP;

  @CommandLine.Option(names = {"-help", "-h", "--h", "--help"}, required = false, usageHelp = true,
      description = "print this message")
  private boolean _help = false;

  private int _numRows = 0;

  public void run()
      throws Exception {
    if (_segmentDir == null && _dataFile == null) {
      System.out.println("Error: One of 'segmentDir' or 'dataFile' must be specified");
      return;
    }

    File segmentFile = (_segmentDir == null) ? buildSegment() : new File(_segmentDir);
    IndexSegment segment = ImmutableSegmentLoader.load(segmentFile, ReadMode.valueOf(_loadMode));
    compareIndexSizes(segment, segmentFile, _fwdIndexColumn, _rawIndexColumn);
    compareLookups(segment);

    // Cleanup the temporary directory
    if (_segmentDir != null) {
      FileUtils.deleteQuietly(new File(SEGMENT_DIR_NAME));
    }
    segment.destroy();
  }

  /**
   * Helper method that builds a segment containing two columns both with data from input file.
   * The first column has raw indices (no dictionary), where as the second column is dictionary encoded.
   *
   * @throws Exception
   */
  private File buildSegment()
      throws Exception {
    Schema schema = new Schema();

    for (int i = 0; i < NUM_COLUMNS; i++) {
      String column = "column_" + i;
      DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec(column, FieldSpec.DataType.STRING, true);
      schema.addField(dimensionFieldSpec);
    }
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName("test").build();
    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    config.setRawIndexCreationColumns(Collections.singletonList(_rawIndexColumn));

    config.setOutDir(SEGMENT_DIR_NAME);
    config.setSegmentName(SEGMENT_NAME);

    BufferedReader reader = new BufferedReader(new FileReader(_dataFile));
    String value;

    final List<GenericRow> rows = new ArrayList<>();

    System.out.println("Reading data...");
    while ((value = reader.readLine()) != null) {
      HashMap<String, Object> map = new HashMap<>();

      for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        map.put(fieldSpec.getName(), value);
      }

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
      _numRows++;

      if (_numRows % 1000000 == 0) {
        System.out.println("Read rows: " + _numRows);
      }
    }

    System.out.println("Generating segment...");
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows));
    driver.build();

    return new File(SEGMENT_DIR_NAME, SEGMENT_NAME);
  }

  /**
   * Compares and prints the index size for the raw and dictionary encoded columns.
   *
   * @param segment Segment to compare
   */
  private void compareIndexSizes(IndexSegment segment, File segmentDir, String fwdIndexColumn, String rawIndexColumn) {
    String filePrefix = segmentDir.getAbsolutePath() + File.separator;
    File rawIndexFile = new File(filePrefix + rawIndexColumn + V1Constants.Indexes.RAW_SV_FORWARD_INDEX_FILE_EXTENSION);

    String extension = (segment.getDataSource(_fwdIndexColumn).getDataSourceMetadata().isSorted())
        ? V1Constants.Indexes.SORTED_SV_FORWARD_INDEX_FILE_EXTENSION
        : V1Constants.Indexes.UNSORTED_SV_FORWARD_INDEX_FILE_EXTENSION;

    File fwdIndexFile = new File(filePrefix + _fwdIndexColumn + extension);
    File fwdIndexDictFile = new File(filePrefix + _fwdIndexColumn + V1Constants.Dict.FILE_EXTENSION);

    long rawIndexSize = rawIndexFile.length();
    long fwdIndexSize = fwdIndexFile.length() + fwdIndexDictFile.length();

    System.out.println("Raw index size: " + toMegaBytes(rawIndexSize) + " MB.");
    System.out.println("Fwd index size: " + toMegaBytes(fwdIndexSize) + " MB.");
    System.out.println("Storage space saving: " + ((fwdIndexSize - rawIndexSize) * 100.0 / fwdIndexSize) + " %");
  }

  /**
   * Compares lookup times for the two columns.
   * Performs {@link #_numConsecutiveLookups} on the two columns on randomly generated docIds.
   *
   * @param segment Segment to compare the columns for
   */
  private void compareLookups(IndexSegment segment) {
    int[] filteredDocIds = generateDocIds(segment);
    long rawIndexTime = profileLookups(segment, _rawIndexColumn, filteredDocIds);
    long fwdIndexTime = profileLookups(segment, _fwdIndexColumn, filteredDocIds);

    System.out.println("Raw index lookup time: " + rawIndexTime);
    System.out.println("Fwd index lookup time: " + fwdIndexTime);
    System.out.println("Percentage change: " + ((fwdIndexTime - rawIndexTime) * 100.0 / rawIndexTime) + " %");
  }

  /**
   * Profiles the lookup time for a given column, for the given docIds.
   *
   * @param segment Segment to profile
   * @param column Column to profile
   * @param docIds DocIds to lookup on the column
   * @return Time take in millis for the lookups
   */
  private long profileLookups(IndexSegment segment, String column, int[] docIds) {
    BaseFilterOperator filterOperator = new TestFilterOperator(docIds, -1);
    DocIdSetOperator docIdSetOperator = new DocIdSetOperator(filterOperator, DocIdSetPlanNode.MAX_DOC_PER_CALL);
    ProjectionOperator projectionOperator = new ProjectionOperator(buildDataSourceMap(segment), docIdSetOperator);

    long start = System.currentTimeMillis();
    ProjectionBlock projectionBlock;
    while ((projectionBlock = projectionOperator.nextBlock()) != null) {
      ProjectionBlockValSet blockValueSet = (ProjectionBlockValSet) projectionBlock.getBlockValueSet(column);
      blockValueSet.getDoubleValuesSV();
    }
    return (System.currentTimeMillis() - start);
  }

  /**
   * Convert from bytes to mega-bytes.
   *
   * @param sizeInBytes Size to convert
   * @return Size in MB's
   */
  private double toMegaBytes(long sizeInBytes) {
    return sizeInBytes / (1024 * 1024);
  }

  /**
   * Helper method to build map from column to data source
   *
   * @param segment Segment for which to build the map
   * @return Column to data source map
   */
  private Map<String, DataSource> buildDataSourceMap(IndexSegment segment) {
    Map<String, DataSource> dataSourceMap = new HashMap<>();
    for (String column : segment.getPhysicalColumnNames()) {
      dataSourceMap.put(column, segment.getDataSource(column));
    }
    return dataSourceMap;
  }

  /**
   * Generate random docIds.
   * <ul>
   *   <li> Total of {@link #_numLookups} docIds are generated. </li>
   *   <li> DocId's are in clusters containing {@link #_numConsecutiveLookups} ids. </li>
   * </ul>
   * @param segment
   * @return
   */
  private int[] generateDocIds(IndexSegment segment) {
    Random random = new Random();
    int numDocs = segment.getSegmentMetadata().getTotalDocs();
    int maxDocId = numDocs - _numConsecutiveLookups - 1;

    int[] docIdSet = new int[_numLookups];
    int j = 0;
    for (int i = 0; i < (_numLookups / _numConsecutiveLookups); i++) {
      int startDocId = random.nextInt(maxDocId);
      int endDocId = startDocId + _numConsecutiveLookups;

      for (int docId = startDocId; docId < endDocId; docId++) {
        docIdSet[j++] = docId;
      }
    }

    int docId = random.nextInt(maxDocId);
    for (; j < _numLookups; j++) {
      docIdSet[j] = docId++;
    }
    return docIdSet;
  }

  /**
   * Main method for the class. Parses the command line arguments, and invokes the benchmark.
   *
   * @param args Command line arguments.
   * @throws Exception
   */
  public static void main(String[] args)
      throws Exception {
    RawIndexBenchmark benchmark = new RawIndexBenchmark();
    CommandLine commandLine = new CommandLine(benchmark);
    CommandLine.ParseResult result = commandLine.parseArgs(args);
    if (commandLine.isUsageHelpRequested() || result.matchedArgs().size() == 0) {
      commandLine.usage(System.out);
      return;
    }
    benchmark.run();
  }
}
