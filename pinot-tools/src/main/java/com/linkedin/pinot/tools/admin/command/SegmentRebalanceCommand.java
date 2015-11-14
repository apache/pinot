package com.linkedin.pinot.tools.admin.command;

import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.pinot.tools.PinotSegmentRebalancer;


public class SegmentRebalanceCommand extends AbstractBaseCommand implements Command {
  private static final Logger LOGGER = LoggerFactory.getLogger(StartBrokerCommand.class);

  @Option(name = "-zkAddress", required = false, metaVar = "<http>", usage = "HTTP address of Zookeeper.")
  private String _zkAddress = DEFAULT_ZK_ADDRESS;

  @Option(name = "-clusterName", required = false, metaVar = "<String>", usage = "Pinot cluster name.")
  private String _clusterName = "PinotCluster";

  @Option(name = "-tableName", required = true, metaVar = "<String>", usage = "Table name to rebalance", forbids ={"-tenantName"})
  private String _tableName;

  @Option(name = "-tenantName", required = true, metaVar = "<string>", usage = "Name of the tenant. Note All tables belonging this tenant will be rebalanced", forbids ={"-tableName"})
  private String _tenantName;

  @Option(name = "-force", required = true, metaVar = "<boolean>", usage = "Force run rebalance. Useful when a node(s) is down currently")
  private boolean _forceRun;

  @Option(name = "-help", required = false, help = true, aliases = { "-h", "--h", "--help" },
      usage = "Print this message.")
  private boolean _help = false;

  public boolean getHelp() {
    return _help;
  }

  @Override
  public boolean execute() throws Exception {
    PinotSegmentRebalancer rebalancer = new PinotSegmentRebalancer(_zkAddress, _clusterName);
    rebalancer.rebalanceTable(_tableName, _tenantName);
    return true;
  }

  @Override
  public String description() {
    return "Rebalance segments for a single table or all tables belonging to a tenant. This is run after adding new nodes to rebalance the segments";
  }
}
