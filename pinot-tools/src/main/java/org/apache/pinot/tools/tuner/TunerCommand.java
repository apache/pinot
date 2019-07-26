package org.apache.pinot.tools.tuner;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.HashSet;
import org.apache.pinot.tools.AbstractBaseCommand;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.tuner.driver.TunerDriver;
import org.apache.pinot.tools.tuner.meta.manager.JsonFileMetaManagerImpl;
import org.apache.pinot.tools.tuner.query.src.LogQuerySrcImpl;
import org.apache.pinot.tools.tuner.query.src.parser.BrokerLogParserImpl;
import org.apache.pinot.tools.tuner.strategy.ParserBasedImpl;
import org.kohsuke.args4j.Option;


public class TunerCommand extends AbstractBaseCommand implements Command {
  public static final long DEFAULT_NUM_ENTRIES_SCANNED_THRESHOLD = 0;
  public static final long DEFAULT_NUM_QUERIES_TO_GIVE_RECOMMENDATION = 0;

  public static final String INVERTED_INDEX = "inverted";
  public static final String SORTED_INDEX = "sorted";
  public static final String OPTIMIZATION = "optimization";

  @Option(name = "-metaDataDir", required = true, metaVar = "<String>", usage = "Path to packed metadata file (json), which contains the weighted sum of cardinality, number of documents, number of entries, etc.")
  private String _metaData;

  @Option(name = "-brokerLog", required = true, metaVar = "<String>", usage = "Path to broker log file containing time of execution, numEntriesScannedInFilter, numEntriesScannedPostFilter, query text.")
  private String _brokerLog;

  @Option(name = "-strategy", required = true, metaVar = "<String>", usage = "Select execution strategy (percentile/inverted/sorted)")
  private String _strategy;

  @Option(name = "-numEntriesScannedThreshold", required = false, metaVar = "<long>", usage = "The threshold for numEntriesScannedInFilter, log lines with numEntriesScannedInFilter below this threshold will be excluded.")
  private long _numEntriesScannedThreshold = DEFAULT_NUM_ENTRIES_SCANNED_THRESHOLD;

  @Option(name = "-numQueriesToGiveRecommendation", required = false, metaVar = "<long>", usage = "The threshold for numEntriesScannedInFilter, log lines with numEntriesScannedInFilter below this threshold will be excluded.")
  private long _numQueriesToGiveRecommendation = DEFAULT_NUM_QUERIES_TO_GIVE_RECOMMENDATION;

  @Option(name = "-tables", required = false, usage = "Comma separated list of table names to work on without type (leave this blank to run on all tables)")
  private String _tableNamesWithoutType = null;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help;

  @Override
  public boolean execute() {
    HashSet<String> tableNamesWithoutType = null;
    if (_tableNamesWithoutType != null) {
      tableNamesWithoutType.addAll(Arrays.asList(_tableNamesWithoutType.split(",")));
    }

    if (_strategy.equals(INVERTED_INDEX)) {
      TunerDriver parserBased = new TunerTest().setThreadPoolSize(Runtime.getRuntime().availableProcessors() - 1)
          .setStrategy(new ParserBasedImpl.Builder().setTableNamesWorkonWithoutType(tableNamesWithoutType)
              .setNumProcessedThreshold(_numQueriesToGiveRecommendation).setAlgorithmOrder(ParserBasedImpl.FIRST_ORDER)
              .setNumEntriesScannedThreshold(_numEntriesScannedThreshold).build())
          .setQuerySrc(new LogQuerySrcImpl.Builder().setParser(new BrokerLogParserImpl()).setPath(_brokerLog).build())
          .setMetaManager(new JsonFileMetaManagerImpl.Builder().setPath(_metaData).build());
      parserBased.execute();
    } else if (_strategy.equals(SORTED_INDEX)) {
      TunerDriver parserBased = new TunerTest().setThreadPoolSize(Runtime.getRuntime().availableProcessors() - 1)
          .setStrategy(new ParserBasedImpl.Builder().setTableNamesWorkonWithoutType(tableNamesWithoutType)
              .setNumProcessedThreshold(_numQueriesToGiveRecommendation).setAlgorithmOrder(ParserBasedImpl.SECOND_ORDER)
              .setNumEntriesScannedThreshold(_numEntriesScannedThreshold).build())
          .setQuerySrc(new LogQuerySrcImpl.Builder().setParser(new BrokerLogParserImpl()).setPath(_brokerLog).build())
          .setMetaManager(new JsonFileMetaManagerImpl.Builder().setPath(_metaData).build());
    } else if (_strategy.equals(OPTIMIZATION)) {

    } else {
      return false;
    }
    return true;
  }

  @Override
  public String description() {
    return "Give optimization boundary analysis and indexing recommendation to specific tables, based on packed segment metadata() and broker logs.";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }
}