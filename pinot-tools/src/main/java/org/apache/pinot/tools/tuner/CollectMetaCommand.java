package org.apache.pinot.tools.tuner;

import org.apache.pinot.tools.AbstractBaseCommand;
import org.apache.pinot.tools.Command;
import org.kohsuke.args4j.Option;


public class CollectMetaCommand extends AbstractBaseCommand implements Command {
  @Option(name = "-workDir", required = true, metaVar = "<String>", usage = "The directory to work on, for temporary files and output metadata.json file，must have r/w access")
  private String _workDir;

  @Option(name = "-segmentsDir", required = true, metaVar = "<String>", usage = "The directory, which contains tableNames/{tarred segments}")
  private String _segmentsDir;

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
