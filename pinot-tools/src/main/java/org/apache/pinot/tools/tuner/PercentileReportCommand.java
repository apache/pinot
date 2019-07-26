package org.apache.pinot.tools.tuner;

import java.util.Arrays;
import java.util.HashSet;
import org.apache.pinot.tools.AbstractBaseCommand;
import org.apache.pinot.tools.Command;
import org.apache.pinot.tools.tuner.driver.TunerDriver;
import org.apache.pinot.tools.tuner.query.src.LogQuerySrcImpl;
import org.apache.pinot.tools.tuner.query.src.parser.BrokerLogParserImpl;
import org.apache.pinot.tools.tuner.strategy.OLSAnalysisImpl;
import org.kohsuke.args4j.Option;


public class PercentileReportCommand extends AbstractBaseCommand implements Command {

  @Option(name = "-brokerLog", required = true, metaVar = "<String>", usage = "Path to broker log file containing time of execution, numEntriesScannedInFilter, numEntriesScannedPostFilter, query text.")
  private String _brokerLog;

  @Option(name = "-tables", required = false, usage = "Comma separated list of table names to work on without type (leave this blank to run on all tables)")
  private String _tableNamesWithoutType = null;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help;

  @Override
  public boolean execute()
      throws Exception {
    HashSet<String> tableNamesWithoutType = new HashSet<>();
    if (_tableNamesWithoutType != null && !_tableNamesWithoutType.trim().equals("")) {
      tableNamesWithoutType.addAll(Arrays.asList(_tableNamesWithoutType.split(",")));
    }

    TunerDriver fitModel = new TunerTest().setThreadPoolSize(Runtime.getRuntime().availableProcessors() - 1)
        .setStrategy(new OLSAnalysisImpl.Builder().setTableNamesWorkonWithoutType(tableNamesWithoutType).build())
        .setQuerySrc(new LogQuerySrcImpl.Builder().setValidLineBeginnerRegex(LogQuerySrcImpl.REGEX_VALID_LINE_TIME)
            .setParser(new BrokerLogParserImpl()).setPath(_brokerLog).build());
    fitModel.execute();
    return true;
  }

  @Override
  public String description() {
    return "Scan through broker log and give percentile of numEntriesScannedInFilter";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }
}
