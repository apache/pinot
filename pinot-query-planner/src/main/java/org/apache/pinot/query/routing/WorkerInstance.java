package org.apache.pinot.query.routing;

import java.util.Map;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * WorkerInstance is a wrapper to enable host-port initialization publicly.
 */
public class WorkerInstance extends ServerInstance {

  public WorkerInstance(InstanceConfig instanceConfig) {
    super(instanceConfig);
  }

  public WorkerInstance(String hostname, int serverPort, int mailboxPort) {
    super(toInstanceConfig(hostname, serverPort, mailboxPort));
  }

  private static InstanceConfig toInstanceConfig(String hostname, int serverPort, int mailboxPort) {
    String server = String.format("%s_%d", hostname, serverPort);
    InstanceConfig instanceConfig = InstanceConfig.toInstanceConfig(server);
    ZNRecord znRecord = instanceConfig.getRecord();
    Map<String, String> simpleFields = znRecord.getSimpleFields();
    simpleFields.put(CommonConstants.Helix.Instance.GRPC_PORT_KEY, String.valueOf(mailboxPort));
    return instanceConfig;
  }
}
