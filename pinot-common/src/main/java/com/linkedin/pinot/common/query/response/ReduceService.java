package com.linkedin.pinot.common.query.response;

import java.util.Map;

import com.linkedin.pinot.common.request.BrokerRequest;


public interface ReduceService {
  /**
   * Reduce method instanceResponses gathered from server instances to one brokerResponse.
   * ServerInstance would be helpful in debug mode
   * 
   * @param brokerRequest
   * @param instanceResponseMap
   * @return BrokerResponse
   */
  public BrokerResponse reduce(BrokerRequest brokerRequest, Map<ServerInstance, InstanceResponse> instanceResponseMap);
}
