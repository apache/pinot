package org.apache.pinot.common.broker;

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;


public abstract class BrokerInfoSelector implements BrokerSelector {

  protected final boolean _preferTlsPort;

  public BrokerInfoSelector(boolean preferTlsPort) {
    _preferTlsPort = preferTlsPort;
  }

  @Nullable
  @Override
  public String selectBroker(String... tableNames) {
    BrokerInfo brokerInfo = selectBrokerInfo(tableNames);
    if (brokerInfo != null) {
      return brokerInfo.getHostPort(_preferTlsPort);
    }
    return null;
  }


  @Override
  public List<String> getBrokers() {
    return getBrokerInfoList().stream().map(b -> b.getHostPort(_preferTlsPort)).collect(Collectors.toList());
  }

  /**
   * Returns a BrokerInfo object with network information about the broker.
   * @param tableNames List of tables that the broker has to process.
   * @return A BrokerInfo object or null if none found.
   */
  public abstract BrokerInfo selectBrokerInfo(String... tableNames);

  /**
   * Get a list of BrokerInfo objects.
   * @return A list of BrokerInfo objects
   */
  public abstract List<BrokerInfo> getBrokerInfoList();
}
