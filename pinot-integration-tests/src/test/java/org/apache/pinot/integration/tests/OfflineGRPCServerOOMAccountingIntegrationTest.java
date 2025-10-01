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
package org.apache.pinot.integration.tests;

import org.apache.pinot.core.accounting.PerQueryCPUMemAccountantFactory;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Accounting;
import org.apache.pinot.spi.utils.CommonConstants.Broker;
import org.apache.pinot.spi.utils.CommonConstants.Server;


public class OfflineGRPCServerOOMAccountingIntegrationTest extends OfflineGRPCServerIntegrationTest {

  @Override
  protected void overrideBrokerConf(PinotConfiguration brokerConf) {
    brokerConf.setProperty(Broker.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT, true);
    brokerConf.setProperty(Broker.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);

    String prefix = CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + ".";
    brokerConf.setProperty(prefix + Accounting.CONFIG_OF_FACTORY_NAME, PerQueryCPUMemAccountantFactory.class.getName());
    brokerConf.setProperty(prefix + Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING, true);
    brokerConf.setProperty(prefix + Accounting.CONFIG_OF_CPU_TIME_BASED_KILLING_ENABLED, true);
    brokerConf.setProperty(prefix + Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    brokerConf.setProperty(prefix + Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, true);
  }

  @Override
  protected void overrideServerConf(PinotConfiguration serverConf) {
    serverConf.setProperty(Server.CONFIG_OF_ENABLE_THREAD_CPU_TIME_MEASUREMENT, true);
    serverConf.setProperty(Server.CONFIG_OF_ENABLE_THREAD_ALLOCATED_BYTES_MEASUREMENT, true);

    String prefix = CommonConstants.PINOT_QUERY_SCHEDULER_PREFIX + ".";
    serverConf.setProperty(prefix + Accounting.CONFIG_OF_FACTORY_NAME, PerQueryCPUMemAccountantFactory.class.getName());
    serverConf.setProperty(prefix + Accounting.CONFIG_OF_ENABLE_THREAD_CPU_SAMPLING, true);
    serverConf.setProperty(prefix + Accounting.CONFIG_OF_CPU_TIME_BASED_KILLING_ENABLED, true);
    serverConf.setProperty(prefix + Accounting.CONFIG_OF_ENABLE_THREAD_MEMORY_SAMPLING, true);
    serverConf.setProperty(prefix + Accounting.CONFIG_OF_OOM_PROTECTION_KILLING_QUERY, true);
  }
}
