package org.apache.pinot.client;

import java.util.concurrent.Future;


public class DummyPinotClientTransport implements PinotClientTransport {
  private String _lastQuery;

  @Override
  public BrokerResponse executeQuery(String brokerAddress, String query)
      throws PinotClientException {
    _lastQuery = query;
    return BrokerResponse.empty();
  }

  @Override
  public Future<BrokerResponse> executeQueryAsync(String brokerAddress, String query)
      throws PinotClientException {
    _lastQuery = query;
    return null;
  }

  @Override
  public BrokerResponse executeQuery(String brokerAddress, Request request)
      throws PinotClientException {
    _lastQuery = request.getQuery();
    return BrokerResponse.empty();
  }

  @Override
  public Future<BrokerResponse> executeQueryAsync(String brokerAddress, Request request)
      throws PinotClientException {
    _lastQuery = request.getQuery();
    return null;
  }

  public String getLastQuery() {
    return _lastQuery;
  }
}

