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
import java.util.Map;
import java.util.TreeMap;
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
  }

  @Test
  public void testUpdateInstancePreservesFieldsWrittenAfterInitialRead() {
    Instance original =
        new Instance("localhost", 1234, InstanceType.SERVER, List.of("DefaultTenant_OFFLINE"), null,
            0, 0, 0, 0, false);
    InstanceConfig initiallyReadConfig = InstanceUtils.toHelixInstanceConfig(original);
    initiallyReadConfig.getRecord().setSimpleField("pinotVersion", "old-version");
    initiallyReadConfig.getRecord().setMapField("capabilities", Map.of("pinotVersion", "old-version"));
    when(_helixDataAccessor.getProperty(any(PropertyKey.class))).thenReturn(initiallyReadConfig);

    // Model a participant restart after updateInstance's read but before its write.
    InstanceConfig latestConfig = InstanceUtils.toHelixInstanceConfig(original);
    latestConfig.getRecord().setSimpleField("pinotVersion", "new-version");
    latestConfig.getRecord().setMapField("capabilities", Map.of("pinotVersion", "new-version", "feature", "1"));
    latestConfig.getRecord().setSimpleField(CommonConstants.Helix.QUERIES_DISABLED, Boolean.TRUE.toString());
    ZNRecord[] persistedRecord = new ZNRecord[1];
    when(_helixDataAccessor.updateProperty(any(PropertyKey.class),
        ArgumentMatchers.<DataUpdater<ZNRecord>>any(), any(InstanceConfig.class))).thenAnswer(invocation -> {
          DataUpdater<ZNRecord> updater = invocation.getArgument(1);
          persistedRecord[0] = updater.update(latestConfig.getRecord());
          return true;
        });

    Instance updated =
        new Instance("localhost", 1234, InstanceType.SERVER, List.of("UpdatedTenant_OFFLINE"), null,
            0, 0, 0, 0, false);
    _resourceManager.updateInstance("Server_localhost_1234", updated, false);

    assertNotNull(persistedRecord[0]);
    assertEquals(persistedRecord[0].getSimpleField("pinotVersion"), "new-version");
    assertEquals(persistedRecord[0].getMapField("capabilities"),
        Map.of("pinotVersion", "new-version", "feature", "1"));
    assertEquals(persistedRecord[0].getListField(InstanceConfig.InstanceConfigProperty.TAG_LIST.name()),
        List.of("UpdatedTenant_OFFLINE"));
    assertEquals(persistedRecord[0].getSimpleField(CommonConstants.Helix.QUERIES_DISABLED), Boolean.TRUE.toString());
    verify(_helixDataAccessor, never()).setProperty(any(PropertyKey.class), any(InstanceConfig.class));
  }

  @Test
  public void testUpdateInstanceRetriesAgainstLatestRecord() {
    Instance original =
        new Instance("localhost", 1234, InstanceType.SERVER, List.of("DefaultTenant_OFFLINE"), null,
            0, 0, 0, 0, false);
    when(_helixDataAccessor.getProperty(any(PropertyKey.class)))
        .thenReturn(InstanceUtils.toHelixInstanceConfig(original));

    InstanceConfig firstAttemptConfig = InstanceUtils.toHelixInstanceConfig(original);
    firstAttemptConfig.getRecord().setSimpleField("pinotVersion", "v2");
    firstAttemptConfig.getRecord().setMapField("capabilities", Map.of("pinotVersion", "v2", "feature", "1"));
    firstAttemptConfig.getRecord().setSimpleField(CommonConstants.Helix.QUERIES_DISABLED, Boolean.TRUE.toString());

    // A concurrent writer wins the first CAS and re-enables queries before the updater is retried.
    InstanceConfig retryConfig = InstanceUtils.toHelixInstanceConfig(original);
    retryConfig.getRecord().setSimpleField("pinotVersion", "v3");
    retryConfig.getRecord().setMapField("capabilities", Map.of("pinotVersion", "v3", "feature", "2"));
    retryConfig.getRecord().setSimpleField(CommonConstants.Helix.QUERIES_DISABLED, Boolean.FALSE.toString());
    ZNRecord[] attemptedRecords = new ZNRecord[2];
    when(_helixDataAccessor.updateProperty(any(PropertyKey.class),
        ArgumentMatchers.<DataUpdater<ZNRecord>>any(), any(InstanceConfig.class))).thenAnswer(invocation -> {
          DataUpdater<ZNRecord> updater = invocation.getArgument(1);
          attemptedRecords[0] = updater.update(firstAttemptConfig.getRecord());
          attemptedRecords[1] = updater.update(retryConfig.getRecord());
          return true;
        });

    Instance updated =
        new Instance("localhost", 1234, InstanceType.SERVER, List.of("UpdatedTenant_OFFLINE"), null,
            0, 0, 0, 0, false);
    _resourceManager.updateInstance("Server_localhost_1234", updated, false);

    assertNotNull(attemptedRecords[0]);
    assertEquals(attemptedRecords[0].getSimpleField("pinotVersion"), "v2");
    assertEquals(attemptedRecords[0].getMapField("capabilities"),
        Map.of("pinotVersion", "v2", "feature", "1"));
    assertEquals(attemptedRecords[0].getListField(InstanceConfig.InstanceConfigProperty.TAG_LIST.name()),
        List.of("UpdatedTenant_OFFLINE"));
    assertEquals(attemptedRecords[0].getSimpleField(CommonConstants.Helix.QUERIES_DISABLED), Boolean.TRUE.toString());

    assertNotNull(attemptedRecords[1]);
    assertEquals(attemptedRecords[1].getSimpleField("pinotVersion"), "v3");
    assertEquals(attemptedRecords[1].getMapField("capabilities"),
        Map.of("pinotVersion", "v3", "feature", "2"));
    assertEquals(attemptedRecords[1].getListField(InstanceConfig.InstanceConfigProperty.TAG_LIST.name()),
        List.of("UpdatedTenant_OFFLINE"));
    assertNull(attemptedRecords[1].getSimpleField(CommonConstants.Helix.QUERIES_DISABLED));
  }

  @Test
  public void testUpdateInstanceThrowsWhenWriteFails() {
    Instance instance =
        new Instance("localhost", 1234, InstanceType.SERVER, List.of("DefaultTenant_OFFLINE"), null,
            0, 0, 0, 0, false);
    when(_helixDataAccessor.getProperty(any(PropertyKey.class)))
        .thenReturn(InstanceUtils.toHelixInstanceConfig(instance));
    when(_helixDataAccessor.updateProperty(any(PropertyKey.class),
        ArgumentMatchers.<DataUpdater<ZNRecord>>any(), any(InstanceConfig.class))).thenReturn(false);

    RuntimeException exception = expectThrows(RuntimeException.class,
        () -> _resourceManager.updateInstance("Server_localhost_1234", instance, false));
    assertEquals(exception.getMessage(), "Failed to update instance config for instance: Server_localhost_1234");
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

  private void setField(String fieldName, Object value)
      throws Exception {
    Field field = PinotHelixResourceManager.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(_resourceManager, value);
  }
}
