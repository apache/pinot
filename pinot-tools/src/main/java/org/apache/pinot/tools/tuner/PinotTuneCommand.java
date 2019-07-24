package org.apache.pinot.tools.tuner;

import org.apache.pinot.tools.AbstractBaseCommand;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.Option;


public class PinotTuneCommand extends AbstractBaseCommand implements Command {

  @Option(name = "-metaData", required = true, metaVar = "<String>", usage = "Path to packed metadata file (json)")
  private String _metaData;

  @Option(name = "-brokerLog", required = true, metaVar = "<String>", usage = "Path to broker log file")
  private String _brokerLog;

  @Option(name = "-strategy", required = true, metaVar = "<String>", usage = "Select execution strategy (percentile/inverted/sorted)")
  private String _stat;

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