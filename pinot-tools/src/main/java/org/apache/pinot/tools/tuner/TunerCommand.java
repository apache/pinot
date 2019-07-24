package org.apache.pinot.tools.tuner;

import org.apache.pinot.tools.AbstractBaseCommand;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.Option;


public class TunerCommand extends AbstractBaseCommand implements Command {

  @Option(name = "-metaDataDir", required = true, metaVar = "<String>", usage = "Path to packed metadata file (json), which contains the weighted sum of cardinality, number of documents, number of entries, etc.")
  private String _metaData;

  @Option(name = "-brokerLog", required = true, metaVar = "<String>", usage = "Path to broker log file containing time of execution, numEntriesScannedInFilter, numEntriesScannedPostFilter, query text.")
  private String _brokerLog;

  @Option(name = "-strategy", required = true, metaVar = "<string>", usage = "Select execution strategy (?percentile/inverted/sorted)")
  private String _strategy;

  @Option(name = "-numEntriesScannedThreshold", required = false, metaVar = "<long>", usage = "The threshold for numEntriesScannedInFilter, log lines with numEntriesScannedInFilter below this threshold will be excluded.")
  private long _numEntriesScannedThreshold;

  @Option(name = "-tables", required = false, usage = "Comma separated list of table names to work on without type (leave this blank to run on all tables)")
  private String _tableNamesWithoutType = null;

  @Option(name = "-help", required = false, help = true, aliases = {"-h", "--h", "--help"}, usage = "Print this message.")
  private boolean _help;

  @Override
  public boolean execute() {
    return true;
  }

  @Override
  public String description() {
    return "";
  }

  @Override
  public boolean getHelp() {
    return _help;
  }
}