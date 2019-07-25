package org.apache.pinot.tools.tuner.meta.manager.metadata.collector;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import javax.annotation.Nonnull;
import org.apache.pinot.tools.tuner.meta.manager.MetaManager;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.AbstractQueryStats;
import org.apache.pinot.tools.tuner.strategy.AbstractAccumulator;
import org.apache.pinot.tools.tuner.strategy.Strategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AccumulateStats implements Strategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(AccumulateStats.class);
  private static final String UNTAR = "tar -xvf ";
  private static final String EXCLUDE_DATA = " --exclude \"columns.psf\" ";
  private static final String OUT_PUT_PATH = " -C ";

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
    return _tableNamesWithoutType.contains(((PathWrapper) filePaths).getTableNameWithoutType());
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
    try {
      Runtime.getRuntime().exec(
          UNTAR + pathWrapper.getFile().getAbsolutePath() + EXCLUDE_DATA + OUT_PUT_PATH + _outputDir.getAbsolutePath());
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
      return;
    }

    File versionFolder;
    File metaDataProperties;
    File indexMap;

    try {
      versionFolder = _outputDir.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.equals(pathWrapper.getFile().getName());
        }
      })[0].listFiles()[0];
    } catch (NullPointerException e) {
      LOGGER.error(e.getMessage());
      return;
    }

    try {
      metaDataProperties = versionFolder.listFiles(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.equals("metadata.properties");
        }
      })[0];
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
    } catch (NullPointerException e) {
      LOGGER.error(e.getMessage());
      indexMap = null;
    }


  }

  /**
   * merge two AbstractMergerObj with same /tableName/colName
   * @param abstractAccumulator input
   * @param abstractAccumulatorToMerge input
   */
  @Override
  public void merge(AbstractAccumulator abstractAccumulator, AbstractAccumulator abstractAccumulatorToMerge) {

  }

  /**
   * Generate a report for recommendation using mergedOut:/colName/AbstractMergerObj
   * @param mergedOut input
   */
  @Override
  public void report(Map<String, Map<String, AbstractAccumulator>> mergedOut) {

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
