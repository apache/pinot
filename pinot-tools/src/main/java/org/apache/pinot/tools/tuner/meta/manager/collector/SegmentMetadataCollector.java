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
package org.apache.pinot.tools.tuner.meta.manager.collector;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.tools.tuner.meta.manager.MetaManager;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.AbstractQueryStats;
import org.apache.pinot.tools.tuner.strategy.AbstractAccumulator;
import org.apache.pinot.tools.tuner.strategy.TuningStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Extract relevant fields in metadata.properties and index_map from tarred segments in controller
 * And pack them into a json file
 */
public class SegmentMetadataCollector implements TuningStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentMetadataCollector.class);
  private static final String UNTAR = "tar -xf ";
  private static final String TMP_THREAD_FILE_PREFIX = "/tmpThreadFile";
  private static final String EXCLUDE_DATA = " --exclude columns.psf ";
  private static final String STRIP_PATHS = " --xform s#^.+/##x ";
  private static final String OUT_PUT_PATH = " -C ";
  private static final String RM_RF = "rm -rf ";

  private static final int REGEX_FIRST_GROUP = 1;
  private static final String REGEX_COL_EXTRACT = "column\\.(.*)\\.columnType = (DIMENSION|TIME)";
  private static final String REGEX_COLUMN = "column\\.";
  private static final String REGEX_CARDINALITY = "\\.cardinality = (.*)";
  private static final String REGEX_TOTAL_DOCS = "\\.totalDocs = (.*)";
  private static final String REGEX_IS_SORTED = "\\.isSorted = (.*)";
  private static final String REGEX_TOTAL_NUMBER_OF_ENTRIES = "\\.totalNumberOfEntries = (.*)";
  private static final String REGEX_INVERTED_INDEX_SIZE = "\\.inverted_index\\.size = (.*)";

  private HashSet<String> _tableNamesWithoutType;
  private File _outputDir;

  public SegmentMetadataCollector(Builder builder) {
    _tableNamesWithoutType = builder._tableNamesWithoutType;

    if (builder._outputDir == null) {
      LOGGER.error("No output path specified!");
      return;
    }
    _outputDir = new File(builder._outputDir);
    if (!_outputDir.exists() || _outputDir.isFile() || !_outputDir.canRead() || !_outputDir.canWrite()) {
      LOGGER.error("Error in output path specified!");
      return;
    }
  }

  /**
   * Filter out irrelevant tables' segments.
   * @param filePaths the (tableNamesWithoutType,segmentPaths) extracted and parsed from directory
   */
  @Override
  public boolean filter(AbstractQueryStats filePaths) {
    return _tableNamesWithoutType == null || _tableNamesWithoutType.isEmpty() || _tableNamesWithoutType.contains(((PathWrapper) filePaths).getTableNameWithoutType());
  }

  /**
   * Accumulate the parsed metadata.properties and index_map to corresponding entry
   * @param filePaths input, {@link PathWrapper}
   * @param metaManager null,
   * @param AccumulatorOut output, map of /tableMame: String/columnName: String/{@link AbstractAccumulator}
   */
  @Override
  public void accumulate(AbstractQueryStats filePaths, MetaManager metaManager, Map<String, Map<String, AbstractAccumulator>> AccumulatorOut) {
    PathWrapper pathWrapper = ((PathWrapper) filePaths);

    File tmpFolder = new File(
        _outputDir.getAbsolutePath() + TMP_THREAD_FILE_PREFIX + Thread.currentThread().getId() + "_" + (System.currentTimeMillis() % 1000000));
    LOGGER.info("Extracting: " + pathWrapper.getFile().getAbsolutePath() + " to " + tmpFolder.getAbsolutePath());

    try {
      try {
        tmpFolder.mkdirs();
        Process p = Runtime.getRuntime()
            .exec(
                (UNTAR + pathWrapper.getFile().getAbsolutePath() + EXCLUDE_DATA + STRIP_PATHS + OUT_PUT_PATH + tmpFolder
                    .getAbsolutePath()));
        p.waitFor();
      } catch (IOException | InterruptedException e) {
        LOGGER.error("Error while extracting {}", pathWrapper.getFile().getAbsolutePath());
        return;
      }

      File metaDataProperties;
      File indexMap;
      try {
        metaDataProperties = tmpFolder.listFiles((dir, name) -> name.equals("metadata.properties"))[0];
        if (!metaDataProperties.exists()) {
          throw new NullPointerException();
        }
      } catch (NullPointerException | ArrayIndexOutOfBoundsException e) {
        LOGGER.error("No metadata.properties file for {}!", pathWrapper.getFile().getAbsolutePath());
        return;
      }

      try {
        indexMap = tmpFolder.listFiles((dir, name) -> name.equals("index_map"))[0];
        if (!indexMap.exists()) {
          throw new NullPointerException();
        }
      } catch (NullPointerException | ArrayIndexOutOfBoundsException e) {
        LOGGER.error("No index_map file for {}!", pathWrapper.getFile().getAbsolutePath());
        indexMap = null;
      }

      String metadataString = "";
      try {
        metadataString = FileUtils.readFileToString(metaDataProperties);
      } catch (IOException | NullPointerException e) {
        LOGGER.error("No metadata.properties for {}!", pathWrapper.getFile().getAbsolutePath());
        return;
      }

      String indexMapString = "";
      try {
        indexMapString = FileUtils.readFileToString(indexMap);
      } catch (IOException | NullPointerException e) {
        LOGGER.error("No index_map for {}!", pathWrapper.getFile().getAbsolutePath());
        indexMapString = "";
      }

      Matcher colMatcher = Pattern.compile(REGEX_COL_EXTRACT).matcher(metadataString);
      while (colMatcher.find()) {
        String colName = colMatcher.group(REGEX_FIRST_GROUP);
        AccumulatorOut.putIfAbsent(pathWrapper.getTableNameWithoutType(), new HashMap<>());
        AccumulatorOut.get(pathWrapper.getTableNameWithoutType()).putIfAbsent(colName, new ColStatsAccumulatorObj());

        Matcher cardinalityMatcher =
            Pattern.compile(REGEX_COLUMN + colName + REGEX_CARDINALITY).matcher(metadataString);
        String cardinality = cardinalityMatcher.find() ? cardinalityMatcher.group(REGEX_FIRST_GROUP) : "1";

        Matcher totalDocsMatcher = Pattern.compile(REGEX_COLUMN + colName + REGEX_TOTAL_DOCS).matcher(metadataString);
        String totalDocs = totalDocsMatcher.find() ? totalDocsMatcher.group(REGEX_FIRST_GROUP) : "0";

        Matcher isSortedMatcher = Pattern.compile(REGEX_COLUMN + colName + REGEX_IS_SORTED).matcher(metadataString);
        String isSorted = isSortedMatcher.find() ? isSortedMatcher.group(REGEX_FIRST_GROUP) : "false";

        Matcher totalNumberOfEntriesMatcher =
            Pattern.compile(REGEX_COLUMN + colName + REGEX_TOTAL_NUMBER_OF_ENTRIES).matcher(metadataString);
        String totalNumberOfEntries =
            totalNumberOfEntriesMatcher.find() ? totalNumberOfEntriesMatcher.group(REGEX_FIRST_GROUP) : "0";

        Matcher invertedIndexSizeMatcher = Pattern.compile(colName + REGEX_INVERTED_INDEX_SIZE).matcher(indexMapString);
        String invertedIndexSize =
            invertedIndexSizeMatcher.find() ? invertedIndexSizeMatcher.group(REGEX_FIRST_GROUP) : "0";

        ((ColStatsAccumulatorObj) AccumulatorOut.get(pathWrapper.getTableNameWithoutType())
            .get(colName)).setCardinality(cardinality)
            .setInvertedIndexSize(invertedIndexSize)
            .setIsSorted(isSorted)
            .setSegmentName(pathWrapper.getFile().getName())
            .setTotalDocs(totalDocs)
            .setTotalNumberOfEntries(totalNumberOfEntries)
            .merge();
      }
    } finally {
      deleteTmp(tmpFolder);
    }
  }

  private void deleteTmp(File tmpFolder) {
    try {
      FileUtils.deleteDirectory(tmpFolder);
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
    }
  }

  /**
   * merge two {@link ColStatsAccumulatorObj} with same /tableName/colName
   * @param abstractAccumulator input
   * @param abstractAccumulatorToMerge input
   */
  @Override
  public void merge(AbstractAccumulator abstractAccumulator, AbstractAccumulator abstractAccumulatorToMerge) {
    ((ColStatsAccumulatorObj) abstractAccumulator).merge((ColStatsAccumulatorObj) abstractAccumulatorToMerge);
  }

  /**
   * Generate a json packing all metadata properties.
   * @param tableResults input
   */
  @Override
  public void report(Map<String, Map<String, AbstractAccumulator>> tableResults) {
    Map<String, Map> packedFile = new HashMap<>();
    Map<String, Map<String, Map<String, BigInteger>>> colMeta = new HashMap<>();
    Map<String, Map<String, Map<String, Map<String, String>>>> segmentMeta = new HashMap<>();

    tableResults.forEach((tableNameNoType, colMap) -> colMap.forEach((colName, obj) -> {
      colMeta.putIfAbsent(tableNameNoType, new HashMap<>());
      colMeta.get(tableNameNoType).put(colName, ((ColStatsAccumulatorObj) obj).getAccumulatedStats());

      segmentMeta.putIfAbsent(tableNameNoType, new HashMap<>());
      segmentMeta.get(tableNameNoType).put(colName, ((ColStatsAccumulatorObj) obj).getSegmentStats());
    }));

    packedFile.put("col_meta", colMeta);
    packedFile.put("segment_meta", segmentMeta);

    String json = null;
    try {
      json = JsonUtils.objectToString(packedFile);
    } catch (JsonProcessingException e) {
      LOGGER.error("Cannot convert to json!");
    }

    File file = new File(_outputDir.getAbsolutePath() + "/metadata.json");
    try {
      file.createNewFile();
      FileUtils.writeStringToFile(file, json);
    } catch (IOException e) {
      LOGGER.error("Error writing to the json file: {}", file.getAbsolutePath());
    }
  }

  public static final class Builder {
    private HashSet<String> _tableNamesWithoutType = new HashSet<>();
    private String _outputDir = null;

    public Builder() {
    }

    @Nonnull
    public Builder setTableNamesWithoutType(@Nonnull HashSet<String> val) {
      _tableNamesWithoutType = val;
      return this;
    }

    @Nonnull
    public Builder setOutputDir(@Nonnull String val) {
      _outputDir = val;
      return this;
    }

    @Nonnull
    public SegmentMetadataCollector build() {
      return new SegmentMetadataCollector(this);
    }
  }
}
