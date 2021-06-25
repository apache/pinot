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
package org.apache.pinot.controller;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.api.listeners.ControllerChangeListener;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.Message;
import org.apache.helix.task.TaskDriver;
import org.apache.pinot.common.Utils;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.metrics.ControllerMeter;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.common.metrics.PinotMetricUtils;
import org.apache.pinot.common.metrics.ValidationMetrics;
import org.apache.pinot.common.utils.ServiceStatus;
import org.apache.pinot.common.utils.fetcher.SegmentFetcherFactory;
import org.apache.pinot.common.utils.helix.LeadControllerUtils;
import org.apache.pinot.controller.api.ControllerAdminApiApplication;
import org.apache.pinot.controller.api.access.AccessControlFactory;
import org.apache.pinot.controller.api.events.MetadataEventNotifierFactory;
import org.apache.pinot.controller.api.resources.ControllerFilePathProvider;
import org.apache.pinot.controller.api.resources.InvalidControllerConfigException;
import org.apache.pinot.controller.helix.SegmentStatusChecker;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.controller.helix.core.minion.MinionInstancesCleanupTask;
import org.apache.pinot.controller.helix.core.minion.PinotHelixTaskResourceManager;
import org.apache.pinot.controller.helix.core.minion.PinotTaskManager;
import org.apache.pinot.controller.helix.core.minion.TaskMetricsEmitter;
import org.apache.pinot.controller.helix.core.realtime.PinotLLCRealtimeSegmentManager;
import org.apache.pinot.controller.helix.core.realtime.PinotRealtimeSegmentManager;
import org.apache.pinot.controller.helix.core.realtime.SegmentCompletionManager;
import org.apache.pinot.controller.helix.core.relocation.SegmentRelocator;
import org.apache.pinot.controller.helix.core.retention.RetentionManager;
import org.apache.pinot.controller.helix.core.statemodel.LeadControllerResourceMasterSlaveStateModelFactory;
import org.apache.pinot.controller.helix.core.util.HelixSetupUtils;
import org.apache.pinot.controller.helix.starter.HelixConfig;
import org.apache.pinot.controller.validation.BrokerResourceValidationManager;
import org.apache.pinot.controller.validation.OfflineSegmentIntervalChecker;
import org.apache.pinot.controller.validation.RealtimeSegmentValidationManager;
import org.apache.pinot.core.periodictask.PeriodicTask;
import org.apache.pinot.core.periodictask.PeriodicTaskScheduler;
import org.apache.pinot.core.transport.ListenerConfig;
import org.apache.pinot.core.transport.TlsConfig;
import org.apache.pinot.core.util.ListenerConfigUtil;
import org.apache.pinot.core.util.TlsUtils;
import org.apache.pinot.spi.crypt.PinotCrypterFactory;


/**
 * Default controller startable implementation. Contains methods to start and stop the controller
 */
public class ControllerStarter extends BaseControllerStarter {

  public ControllerStarter() {
  }

  @Deprecated
  public ControllerStarter(PinotConfiguration pinotConfiguration)
      throws Exception {
    init(pinotConfiguration);
  }

  public static ControllerStarter startDefault()
      throws Exception {
    return startDefault(null);
  }

  public static ControllerStarter startDefault(File webappPath)
      throws Exception {
    final ControllerConf conf = new ControllerConf();
    conf.setControllerHost("localhost");
    conf.setControllerPort("9000");
    conf.setDataDir("/tmp/PinotController");
    conf.setZkStr("localhost:2122");
    conf.setHelixClusterName("quickstart");
    if (webappPath == null) {
      String path = ControllerStarter.class.getClassLoader().getResource("webapp").getFile();
      if (!path.startsWith("file://")) {
        path = "file://" + path;
      }
      conf.setQueryConsolePath(path);
    } else {
      conf.setQueryConsolePath("file://" + webappPath.getAbsolutePath());
    }

    conf.setControllerVipHost("localhost");
    conf.setControllerVipProtocol(CommonConstants.HTTP_PROTOCOL);
    conf.setRetentionControllerFrequencyInSeconds(3600 * 6);
    conf.setOfflineSegmentIntervalCheckerFrequencyInSeconds(3600);
    conf.setRealtimeSegmentValidationFrequencyInSeconds(3600);
    conf.setBrokerResourceValidationFrequencyInSeconds(3600);
    conf.setStatusCheckerFrequencyInSeconds(5 * 60);
    conf.setSegmentRelocatorFrequencyInSeconds(3600);
    conf.setStatusCheckerWaitForPushTimeInSeconds(10 * 60);
    conf.setTenantIsolationEnabled(true);

    final ControllerStarter starter = new ControllerStarter();
    starter.init(conf);

    starter.start();
    return starter;
  }

  public static void main(String[] args)
      throws Exception {
    startDefault();
  }
}
