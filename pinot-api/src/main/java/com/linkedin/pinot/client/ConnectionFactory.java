/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.I0Itec.zkclient.IZkConnection;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.helix.manager.zk.ByteArraySerializer;
import org.apache.helix.util.ZKClientPool;
import org.json.JSONObject;


/**
 * Creates connections to Pinot, given various initialization methods.
 *
 * @author jfim
 */
public class ConnectionFactory {
  static PinotClientTransportFactory _transportFactory = new JsonAsyncHttpPinotClientTransportFactory();

  private ConnectionFactory(){}

  /**
   * Creates a connection to a Pinot cluster, given its Zookeeper URL
   *
   * @param zkUrl The URL to the Zookeeper cluster
   * @return A connection that connects to the brokers in the given Helix cluster
   */
  public static Connection fromZookeeper(String zkUrl) {
    try {
      ZkClient client = new ZkClient(zkUrl);
      client.setZkSerializer(new ByteArraySerializer());
      byte[] brokerResourceNodeData = client.readData("/EXTERNALVIEW/brokerResource", true);
      System.out.println("brokerResourceNodeData = " + new String(brokerResourceNodeData));
      JSONObject jsonObject = new JSONObject(new String(brokerResourceNodeData));
      System.out.println("jsonObject = " + jsonObject);
      JSONObject brokerResourceNode = jsonObject.getJSONObject("mapFields");

      List<String> brokerUrls = new ArrayList<String>();

      Iterator<String> resourceNames = brokerResourceNode.keys();
      while (resourceNames.hasNext()) {
        String resourceName = resourceNames.next();
        JSONObject resource = brokerResourceNode.getJSONObject(resourceName);

        Iterator<String> brokerNames = resource.keys();
        while (brokerNames.hasNext()) {
          String brokerName = brokerNames.next();
          if (brokerName.startsWith("Broker_") && "ONLINE".equals(resource.getString(brokerName))) {
            // Turn Broker_12.34.56.78_1234 into 12.34.56.78:1234
            brokerUrls.add(brokerName.replace("Broker_", "").replace("_", ":"));
          }
        }
      }

      client.close();

      return new Connection(brokerUrls, _transportFactory.buildTransport());
    } catch (Exception e) {
      throw new PinotClientException(e);
    }
  }

  /**
   * Creates a connection from properties containing the connection parameters.
   *
   * @param properties The properties to use for the connection
   * @return A connection that connects to the brokers specified in the properties
   */
  public static Connection fromProperties(Properties properties) {
    return new Connection(Arrays.asList(properties.getProperty("brokerList")), _transportFactory.buildTransport());
  }

  /**
   * Creates a connection which sends queries randomly between the specified brokers.
   *
   * @param brokers The list of brokers to send queries to
   * @return A connection to the set of brokers specified
   */
  public static Connection fromHostList(String... brokers) {
    return new Connection(Arrays.asList(brokers), _transportFactory.buildTransport());
  }
}
