package org.apache.pinot.tools.tuner.meta.manager.collector;

import com.google.gson.Gson;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.tools.tuner.meta.manager.MetaManager;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.AbstractQueryStats;
import org.apache.pinot.tools.tuner.strategy.AbstractAccumulator;
import org.apache.pinot.tools.tuner.strategy.Strategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AccumulateStats implements Strategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(AccumulateStats.class);
  private static final String UNTAR = "tar -xf ";
  private static final String EXCLUDE_DATA = " --exclude columns.psf ";
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

  public AccumulateStats(Builder builder) {
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
   * Filter out irrelevant segments
   * @param filePaths the (tableNamesWithoutType,segmentPaths) extracted and parsed from directory
   * @return
   */
  @Override
  public boolean filter(AbstractQueryStats filePaths) {
    return _tableNamesWithoutType.isEmpty() || _tableNamesWithoutType
        .contains(((PathWrapper) filePaths).getTableNameWithoutType());
  }

  /**
   * Accumulate the parsed metadata.properties and index_map to corresponding entry
   * @param filePaths input, PathWrapper
   * @param metaManager null,
   * @param AccumulatorOut output, map of /tableMame: String/columnName: String/AbstractMergerObj
   */
  @Override
  public void accumulate(AbstractQueryStats filePaths, MetaManager metaManager,
      Map<String, Map<String, AbstractAccumulator>> AccumulatorOut) {
    PathWrapper pathWrapper = ((PathWrapper) filePaths);
    LOGGER.info("Extracting: " + pathWrapper.getFile().getAbsolutePath() + " to " + _outputDir.getAbsolutePath());
    try {
      Process p = Runtime.getRuntime().exec(
          (UNTAR + pathWrapper.getFile().getAbsolutePath() + EXCLUDE_DATA + OUT_PUT_PATH + _outputDir
              .getAbsolutePath()));
      p.waitFor();
    } catch (IOException | InterruptedException e) {
      LOGGER.error(e.getMessage());
      return;
    }

    File tmpFolder;
    File versionFolder;
    File metaDataProperties;
    File indexMap;

    try {
      tmpFolder = _outputDir.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.equals(pathWrapper.getFile().getName());
        }
      })[0];
    } catch (NullPointerException e) {
      LOGGER.error("no tmp folder!");
      return;
    }

    try {
      versionFolder = tmpFolder.listFiles()[0];
    } catch (NullPointerException e) {
      LOGGER.error("no version folder!");
      return;
    }

    try {
      metaDataProperties = versionFolder.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.equals("metadata.properties");
        }
      })[0];
      if (!metaDataProperties.exists()) {
        throw new NullPointerException();
      }
    } catch (NullPointerException e) {
      LOGGER.error(e.getMessage());
      return;
    }

    try {
      indexMap = versionFolder.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.equals("index_map");
        }
      })[0];
      if (!indexMap.exists()) {
        throw new NullPointerException();
      }
    } catch (NullPointerException e) {
      LOGGER.error(e.getMessage());
      indexMap = null;
    }

    String metadataString = "";
    try {
      metadataString = FileUtils.readFileToString(metaDataProperties);
    } catch (IOException e) {
      LOGGER.error(e.toString());
      return;
    }

    String indexMapString;
    try {
      indexMapString = FileUtils.readFileToString(indexMap);
    } catch (IOException e) {
      LOGGER.error(e.toString());
      indexMapString = "";
    }

    Matcher colMatcher = Pattern.compile(REGEX_COL_EXTRACT).matcher(metadataString);
    while (colMatcher.find()) {
      String colName = colMatcher.group(REGEX_FIRST_GROUP);
      AccumulatorOut.putIfAbsent(pathWrapper.getTableNameWithoutType(), new HashMap<>());
      AccumulatorOut.get(pathWrapper.getTableNameWithoutType()).putIfAbsent(colName, new ColStatsAccumulatorObj());

      Matcher cardinalityMatcher = Pattern.compile(REGEX_COLUMN + colName + REGEX_CARDINALITY).matcher(metadataString);
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

      ((ColStatsAccumulatorObj) AccumulatorOut.get(pathWrapper.getTableNameWithoutType()).get(colName))
          .addCardinality(cardinality).addInvertedIndexSize(invertedIndexSize).addIsSorted(isSorted)
          .addSegmentName(pathWrapper.getFile().getName()).addTotalDocs(totalDocs)
          .addTotalNumberOfEntries(totalNumberOfEntries).merge();
    }

    try {
      Runtime.getRuntime().exec(RM_RF + tmpFolder.getAbsolutePath());
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
      return;
    }
  }

  /**
   * merge two AbstractMergerObj with same /tableName/colName
   * @param abstractAccumulator input
   * @param abstractAccumulatorToMerge input
   */
  @Override
  public void merge(AbstractAccumulator abstractAccumulator, AbstractAccumulator abstractAccumulatorToMerge) {
    ((ColStatsAccumulatorObj) abstractAccumulator).merge((ColStatsAccumulatorObj) abstractAccumulatorToMerge);
  }

  /**
   * Generate a report for recommendation using mergedOut:tableName/colName/AbstractMergerObj
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
    Gson gson = new Gson();
    String json = gson.toJson(packedFile);

    File file = new File(_outputDir.getAbsolutePath() + "/metadata.json");
    try {
      file.createNewFile();
      FileUtils.writeStringToFile(file, json);
    } catch (IOException e) {
      e.printStackTrace();
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
    public AccumulateStats build() {
      return new AccumulateStats(this);
    }
  }
}
