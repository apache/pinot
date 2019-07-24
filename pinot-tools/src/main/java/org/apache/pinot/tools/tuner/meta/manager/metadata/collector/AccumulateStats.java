package org.apache.pinot.tools.tuner.meta.manager.metadata.collector;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import org.apache.pinot.tools.tuner.meta.manager.MetaManager;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.AbstractQueryStats;
import org.apache.pinot.tools.tuner.strategy.AbstractAccumulator;
import org.apache.pinot.tools.tuner.strategy.Strategy;


public class AccumulateStats implements Strategy {
  private HashSet<String> _tableNamesWithoutType;
  private String _outputType;

  /**
   * Filter out irrelevant query stats to target a specific table or specific range of nESI
   * @param queryStats the stats extracted and parsed from QuerySrc
   * @return
   */
  @Override
  public boolean filter(AbstractQueryStats queryStats) {
    return true;
  }

  /**
   * Accumulate the parsed queryStats to corresponding entry in MapperOut, see FrequencyImpl for ex
   * @param queryStats input, the stats extracted and parsed from QuerySrc
   * @param metaManager input, the metaManager where cardinality info can be get from
   * @param AccumulatorOut output, map of /tableMame: String/columnName: String/AbstractMergerObj
   */
  @Override
  public void accumulate(AbstractQueryStats queryStats, MetaManager metaManager,
      Map<String, Map<String, AbstractAccumulator>> AccumulatorOut) {

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
   * @param tableNameWithoutType input
   * @param mergedOut input
   */
  @Override
  public void report(String tableNameWithoutType, Map<String, AbstractAccumulator> mergedOut) {

  }
}

/*TODO: Test code, this will be deleted*/
class UncompressAndGetMeta {
  String _directory = null;

  public void uncompressFile(String path) {
    try {
      Runtime.getRuntime().exec("tar -xvf " + path + " --exclude \"columns.psf\"");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void remove(String path) {
    try {
      Runtime.getRuntime().exec("rm -rf " + path);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}