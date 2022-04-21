package org.apache.pinot.client;

import java.util.List;
import java.util.Map;


public class BrokerData {
  private final Map<String, List<String>> _tableToBrokerMap;
  private final List<String> _brokers;

  public Map<String, List<String>> getTableToBrokerMap() {
    return _tableToBrokerMap;
  }

  public List<String> getBrokers() {
    return _brokers;
  }

  public BrokerData(Map<String, List<String>> tableToBrokerMap, List<String> brokers) {
    _tableToBrokerMap = tableToBrokerMap;
    _brokers = brokers;
  }
}