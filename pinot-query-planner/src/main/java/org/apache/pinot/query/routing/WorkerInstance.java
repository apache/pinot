/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.query.routing;

import java.util.Map;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.spi.utils.CommonConstants;


/**
 * WorkerInstance is a wrapper around {@link ServerInstance}.
 *
 * <p>This can be considered as a simplified version which directly enable host-port initialization.
 */
public class WorkerInstance extends ServerInstance {

  public WorkerInstance(InstanceConfig instanceConfig) {
    super(instanceConfig);
  }

  public WorkerInstance(String hostname, int nettyPort, int grpcPort, int servicePort, int mailboxPort) {
    super(toInstanceConfig(hostname, nettyPort, grpcPort, servicePort, mailboxPort));
  }

  private static InstanceConfig toInstanceConfig(String hostname, int nettyPort, int grpcPort, int servicePort,
      int mailboxPort) {
    String server = String.format("%s%s_%d", CommonConstants.Helix.PREFIX_OF_SERVER_INSTANCE, hostname, nettyPort);
    InstanceConfig instanceConfig = InstanceConfig.toInstanceConfig(server);
    ZNRecord znRecord = instanceConfig.getRecord();
    Map<String, String> simpleFields = znRecord.getSimpleFields();
    simpleFields.put(CommonConstants.Helix.Instance.GRPC_PORT_KEY, String.valueOf(grpcPort));
    simpleFields.put(CommonConstants.Helix.Instance.MULTI_STAGE_QUERY_ENGINE_SERVICE_PORT_KEY,
        String.valueOf(servicePort));
    simpleFields.put(CommonConstants.Helix.Instance.MULTI_STAGE_QUERY_ENGINE_MAILBOX_PORT_KEY,
        String.valueOf(mailboxPort));
    return instanceConfig;
  }
}
