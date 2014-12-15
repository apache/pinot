package com.linkedin.pinot.common.query;

import java.util.Map;

import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.BrokerResponse;
import com.linkedin.pinot.common.response.InstanceResponse;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.DataTable;


public interface ReduceService {
  /**
   * Reduce instanceResponses gathered from server instances to one brokerResponse.
   * ServerInstance would be helpful in debug mode
   * All the implementations should be thread safe.
   *
   * 
   * @param brokerRequest
   * @param instanceResponseMap
   * @return BrokerResponse
   */
  public BrokerResponse reduce(BrokerRequest brokerRequest, Map<ServerInstance, InstanceResponse> instanceResponseMap);

  /**
   * Reduce instanceResponses gathered from server instances to one brokerResponse.
   * ServerInstance would be helpful in debug mode
   * All the implementations should be thread safe.
   *
   * 
   * @param brokerRequest
   * @param instanceResponseMap
   * @return BrokerResponse
   */
  public BrokerResponse reduceOnDataTable(BrokerRequest brokerRequest,
      Map<ServerInstance, DataTable> instanceResponseMap);

}
