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
package org.apache.pinot.controller.helix.core;

import java.lang.reflect.Field;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.apache.pinot.common.utils.config.InstanceUtils;
import org.apache.pinot.controller.helix.core.lineage.LineageManager;
import org.apache.pinot.spi.config.instance.Instance;
import org.apache.pinot.spi.config.instance.InstanceConfigValidatorRegistry;
import org.apache.pinot.spi.config.instance.InstanceType;
import org.apache.pinot.spi.exception.ConfigValidationException;
import org.apache.pinot.spi.utils.CommonConstants;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class PinotHelixResourceManagerConfigValidationTest {

  private PinotHelixResourceManager _resourceManager;
  private HelixAdmin _helixAdmin;
  private HelixDataAccessor _helixDataAccessor;
  private PropertyKey.Builder _keyBuilder;

  @BeforeMethod
  public void setUp()
      throws Exception {
    LineageManager lineageManager = Mockito.mock(LineageManager.class);
    _resourceManager = new PinotHelixResourceManager("testCluster", null, false, 7, false, lineageManager, null);

    _helixAdmin = Mockito.mock(HelixAdmin.class);
    _helixDataAccessor = Mockito.mock(HelixDataAccessor.class);
    _keyBuilder = Mockito.mock(PropertyKey.Builder.class);
    when(_keyBuilder.instanceConfig(any())).thenReturn(Mockito.mock(PropertyKey.class));
    when(_helixDataAccessor.keyBuilder()).thenReturn(_keyBuilder);

    setField("_helixAdmin", _helixAdmin);
    setField("_helixDataAccessor", _helixDataAccessor);
    setField("_keyBuilder", _keyBuilder);
  }

  @AfterMethod
  public void tearDown() {
    InstanceConfigValidatorRegistry.reset();
  }

  @Test
  public void testAddInstanceRejectsOnValidation() {
    // getHelixInstanceConfig returns null (instance doesn't exist yet)
    when(_helixDataAccessor.getProperty(any(PropertyKey.class))).thenReturn(null);

    InstanceConfigValidatorRegistry.register(instance -> {
      throw new ConfigValidationException("pool tags missing");
    });

    Instance serverInstance =
        new Instance("localhost", 1234, InstanceType.SERVER, null, null, 0, 0, 0, 0, false);
    try {
      _resourceManager.addInstance(serverInstance, false);
      fail("Expected ConfigValidationException");
    } catch (ConfigValidationException e) {
      assertTrue(e.getMessage().contains("pool tags missing"));
    }

    // Verify instance was NOT persisted to Helix
    verify(_helixAdmin, never()).addInstance(any(), any(InstanceConfig.class));
  }

  @Test
  public void testUpdateInstanceRejectsOnValidation() {
    // getHelixInstanceConfig returns an existing config
    Instance existing =
        new Instance("localhost", 1234, InstanceType.SERVER, List.of("DefaultTenant_OFFLINE"), null,
            0, 0, 0, 0, false);
    InstanceConfig existingConfig = InstanceUtils.toHelixInstanceConfig(existing);
    when(_helixDataAccessor.getProperty(any(PropertyKey.class))).thenReturn(existingConfig);

    InstanceConfigValidatorRegistry.register(instance -> {
      throw new ConfigValidationException("pool disjointness violated");
    });

    Instance updatedInstance =
        new Instance("localhost", 1234, InstanceType.SERVER, List.of("OtherTenant_OFFLINE"), null, 0,
            0, 0, 0, false);
    try {
      _resourceManager.updateInstance("Server_localhost_1234", updatedInstance, false);
      fail("Expected ConfigValidationException");
    } catch (ConfigValidationException e) {
      assertTrue(e.getMessage().contains("pool disjointness violated"));
    }

    // Verify config was NOT persisted
    verify(_helixDataAccessor, never()).setProperty(any(PropertyKey.class), any(InstanceConfig.class));
    verify(_helixDataAccessor, never()).updateProperty(any(PropertyKey.class),
        ArgumentMatchers.<DataUpdater<ZNRecord>>any(), any(InstanceConfig.class));
  }

  @Test
  public void testUpdateInstanceTagsRejectsOnValidation() {
    // getHelixInstanceConfig returns an existing server config with pool tags
    TreeMap<String, Integer> pools = new TreeMap<>();
    pools.put("DefaultTenant_OFFLINE", 0);
    Instance existing =
        new Instance("localhost", 1234, InstanceType.SERVER, List.of("DefaultTenant_OFFLINE"), pools,
            0, 0, 0, 0, false);
    InstanceConfig existingConfig = InstanceUtils.toHelixInstanceConfig(existing);
    when(_helixDataAccessor.getProperty(any(PropertyKey.class))).thenReturn(existingConfig);

    InstanceConfigValidatorRegistry.register(instance -> {
      // Verify the validator receives the Instance with NEW tags applied
      assertTrue(instance.getTags().contains("BadTenant_OFFLINE"));
      throw new ConfigValidationException("tenant disjointness violated");
    });

    try {
      _resourceManager.updateInstanceTags("Server_localhost_1234", "BadTenant_OFFLINE", false);
      fail("Expected ConfigValidationException");
    } catch (ConfigValidationException e) {
      assertTrue(e.getMessage().contains("tenant disjointness violated"));
    }

    // Verify config was NOT persisted
    verify(_helixDataAccessor, never()).setProperty(any(PropertyKey.class), any(InstanceConfig.class));
    verify(_helixDataAccessor, never()).updateProperty(any(PropertyKey.class),
        ArgumentMatchers.<DataUpdater<ZNRecord>>any(), any(InstanceConfig.class));
  }

  @Test
  public void testUpdateInstancePreservesConcurrentShutdownMarker() {
    String instanceId = "Broker_localhost_1234";
    Instance existing =
        new Instance("localhost", 1234, InstanceType.BROKER, List.of("DefaultTenant_BROKER"), null,
            0, 0, 0, 0, false);
    InstanceConfig staleConfig = InstanceUtils.toHelixInstanceConfig(existing);
    InstanceConfig latestConfig = new InstanceConfig(new ZNRecord(staleConfig.getRecord()));
    latestConfig.getRecord().setBooleanField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, true);
    when(_helixDataAccessor.getProperty(any(PropertyKey.class))).thenReturn(staleConfig);

    AtomicReference<ZNRecord> persistedRecord = new AtomicReference<>();
    mockAtomicInstanceConfigUpdate(latestConfig, persistedRecord);

    Instance updatedInstance =
        new Instance("updated-host", 4321, InstanceType.BROKER, List.of("OtherTenant_BROKER"), null,
            0, 0, 0, 0, false);
    assertTrue(_resourceManager.updateInstance(instanceId, updatedInstance, false).isSuccessful());

    InstanceConfig persistedConfig = new InstanceConfig(persistedRecord.get());
    assertEquals(persistedConfig.getHostName(), "updated-host");
    assertEquals(persistedConfig.getPort(), "4321");
    assertEquals(persistedConfig.getTags(), List.of());
    assertEquals(persistedConfig.getRecord().getListField(CommonConstants.Helix.PREVIOUS_TAGS),
        List.of("OtherTenant_BROKER"));
    assertTrue(persistedConfig.getRecord()
        .getBooleanField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, false));
    verify(_helixDataAccessor, never()).setProperty(any(PropertyKey.class), any(InstanceConfig.class));
  }

  @Test
  public void testUpdateInstanceTagsPreservesConcurrentShutdownMarker() {
    String instanceId = "Broker_localhost_1234";
    Instance existing =
        new Instance("localhost", 1234, InstanceType.BROKER, List.of("DefaultTenant_BROKER"), null,
            0, 0, 0, 0, false);
    InstanceConfig staleConfig = InstanceUtils.toHelixInstanceConfig(existing);
    InstanceConfig latestConfig = new InstanceConfig(new ZNRecord(staleConfig.getRecord()));
    latestConfig.getRecord().setBooleanField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, true);
    when(_helixDataAccessor.getProperty(any(PropertyKey.class))).thenReturn(staleConfig);

    AtomicReference<ZNRecord> persistedRecord = new AtomicReference<>();
    mockAtomicInstanceConfigUpdate(latestConfig, persistedRecord);

    assertTrue(_resourceManager.updateInstanceTags(instanceId, "OtherTenant_BROKER", false).isSuccessful());

    InstanceConfig persistedConfig = new InstanceConfig(persistedRecord.get());
    assertEquals(persistedConfig.getTags(), List.of());
    assertEquals(persistedConfig.getRecord().getListField(CommonConstants.Helix.PREVIOUS_TAGS),
        List.of("OtherTenant_BROKER"));
    assertTrue(persistedConfig.getRecord()
        .getBooleanField(CommonConstants.Helix.IS_SHUTDOWN_IN_PROGRESS, false));
    verify(_helixDataAccessor, never()).setProperty(any(PropertyKey.class), any(InstanceConfig.class));
  }

  @Test
  public void testAddInstanceSucceedsWithPassingValidator() {
    when(_helixDataAccessor.getProperty(any(PropertyKey.class))).thenReturn(null);

    InstanceConfigValidatorRegistry.register(instance -> {
      // passes — no exception
    });

    Instance serverInstance =
        new Instance("localhost", 5678, InstanceType.SERVER, null, null, 0, 0, 0, 0, false);
    _resourceManager.addInstance(serverInstance, false);

    // Verify instance WAS persisted
    verify(_helixAdmin).addInstance(eq("testCluster"), any(InstanceConfig.class));
  }

  @Test
  public void testMultiWriteZkThrowsBeforeStart() {
    // The fixture never calls start(), so _helixZkManager is null. multiWriteZK() must refuse to
    // build a client — the ZK address is derived from the Helix manager.
    try {
      _resourceManager.multiWriteZK();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException expected) {
      assertTrue(expected.getMessage().contains("not been started"),
          "Unexpected message: " + expected.getMessage());
    }
  }

  private void mockAtomicInstanceConfigUpdate(InstanceConfig latestConfig,
      AtomicReference<ZNRecord> persistedRecord) {
    doAnswer(invocation -> {
      DataUpdater<ZNRecord> updater = invocation.getArgument(1);
      persistedRecord.set(updater.update(new ZNRecord(latestConfig.getRecord())));
      return true;
    }).when(_helixDataAccessor).updateProperty(any(PropertyKey.class),
        ArgumentMatchers.<DataUpdater<ZNRecord>>any(), any(InstanceConfig.class));
  }

  private void setField(String fieldName, Object value)
      throws Exception {
    Field field = PinotHelixResourceManager.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(_resourceManager, value);
  }
}
