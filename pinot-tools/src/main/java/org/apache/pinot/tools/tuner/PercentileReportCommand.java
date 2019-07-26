package org.apache.pinot.tools.tuner;

import org.apache.pinot.tools.AbstractBaseCommand;
import org.apache.pinot.tools.Command;
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
    return false;
  }

  @Override
  public String description() {
    return null;
  }

  @Override
  public boolean getHelp() {
    return false;
  }
}
