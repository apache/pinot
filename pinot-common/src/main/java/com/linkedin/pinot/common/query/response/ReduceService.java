package com.linkedin.pinot.common.query.response;

import java.util.Map;

import com.linkedin.pinot.common.request.BrokerRequest;


public interface ReduceService {
  public BrokerResponse reduce(BrokerRequest brokerRequest, Map<ServerInstance, InstanceResponse> instanceResponseMap);
}
