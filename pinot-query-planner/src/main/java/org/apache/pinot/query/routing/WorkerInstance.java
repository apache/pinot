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

  public WorkerInstance(String hostname, int port) {
    super(toInstanceConfig(hostname, port));
  }

  private static InstanceConfig toInstanceConfig(String hostname, int port) {
    String server = String.format("%s_%d", hostname, port);
    InstanceConfig instanceConfig = InstanceConfig.toInstanceConfig(server);
    ZNRecord znRecord = instanceConfig.getRecord();
    Map<String, String> simpleFields = znRecord.getSimpleFields();
    simpleFields.put(CommonConstants.Helix.Instance.GRPC_PORT_KEY, instanceConfig.getPort());
    return instanceConfig;
  }
}
