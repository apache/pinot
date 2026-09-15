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
package org.apache.pinot.common.utils.config;

import java.util.List;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.config.user.AccessType;
import org.apache.pinot.spi.config.user.UserConfig;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class AccessControlUserConfigUtilsTest {

  @Test
  public void testFineGrainedPermissionsRoundTripInSeparateListField()
      throws Exception {
    UserConfig userConfig = new UserConfig("testUser", "testPassword", "CONTROLLER", "ADMIN",
        List.of("table1"), List.of("excludedTable"), List.of(AccessType.READ), List.of("GetZnode"));

    ZNRecord znRecord = AccessControlUserConfigUtils.toZNRecord(userConfig);
    assertEquals(znRecord.getListField(UserConfig.PERMISSIONS_KEY), List.of("READ"));
    assertEquals(znRecord.getListField(UserConfig.FINE_GRAINED_PERMISSIONS_KEY), List.of("GetZnode"));

    UserConfig deserialized = AccessControlUserConfigUtils.fromZNRecord(znRecord);
    assertEquals(deserialized.getPermissions(), List.of(AccessType.READ));
    assertEquals(deserialized.getFineGrainedPermissions(), List.of("GetZnode"));
  }
}
