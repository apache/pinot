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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.spi.config.user.AccessType;
import org.apache.pinot.spi.config.user.UserConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.slf4j.Logger;


/**
 * UserConfigUtils is responsible for two things:
 * 1. Used to acquire user config by parsing znRecord that stored in Zookeeper
 * 2. Used to construct znRecord by packaging user config
 */
public class AccessControlUserConfigUtils {
    private AccessControlUserConfigUtils() {
    }

    private static final Logger LOGGER = org.slf4j.LoggerFactory.getLogger(
        AccessControlUserConfigUtils.class);

    public static UserConfig fromZNRecord(ZNRecord znRecord) {
        Map<String, String> simpleFields = znRecord.getSimpleFields();

        // Mandatory fields
        String username = simpleFields.get(UserConfig.USERNAME_KEY);
        String password = simpleFields.get(UserConfig.PASSWORD_KEY);
        String component = simpleFields.get(UserConfig.COMPONET_KEY);
        String role = simpleFields.get(UserConfig.ROLE_KEY);

        List<String> tableList = znRecord.getListField(UserConfig.TABLES_KEY);
        List<String> excludeTableList = znRecord.getListField(UserConfig.EXCLUDE_TABLES_KEY);

        List<String> permissionListFromZNRecord = znRecord.getListField(
            UserConfig.PERMISSIONS_KEY);
        List<AccessType> permissionList = null;
        if (permissionListFromZNRecord != null) {
            permissionList = permissionListFromZNRecord.stream()
                .map(x -> AccessType.valueOf(x))
                .collect(Collectors.toList());
        }

        // Extract RLS filters from simple fields (stored as JSON)
        Map<String, List<String>> rlsFilters = null;
        String rlsFiltersJson = simpleFields.get(UserConfig.RLS_FILTERS_KEY);
        if (rlsFiltersJson != null && !rlsFiltersJson.isEmpty()) {
            try {
                rlsFilters = JsonUtils.stringToObject(
                    rlsFiltersJson, new TypeReference<Map<String, List<String>>>() {
                    }
                );
            } catch (IOException e) {
                // Log error but continue - RLS filters are optional
                LOGGER.error("Failed to deserialize RLS filters for user: {}", username, e);
            }
        }

        return new UserConfig(username, password, component, role, tableList,
            excludeTableList, permissionList, rlsFilters);
    }

    public static ZNRecord toZNRecord(UserConfig userConfig)
        throws JsonProcessingException {
        Map<String, String> simpleFields = new HashMap<>();

        // Mandatory fields
        simpleFields.put(UserConfig.USERNAME_KEY, userConfig.getUserName());
        simpleFields.put(UserConfig.PASSWORD_KEY, userConfig.getPassword());
        simpleFields.put(UserConfig.COMPONET_KEY, userConfig.getComponentType().toString());
        simpleFields.put(UserConfig.ROLE_KEY, userConfig.getRoleType().toString());

        Map<String, List<String>> listFields = new HashMap<>();

        // Optional fields
        List<String> tableList = userConfig.getTables();
        if (tableList != null) {
            listFields.put(UserConfig.TABLES_KEY, userConfig.getTables());
        }
        List<String> excludeTableList = userConfig.getExcludeTables();
        if (excludeTableList != null) {
            listFields.put(UserConfig.EXCLUDE_TABLES_KEY, userConfig.getExcludeTables());
        }

        List<AccessType> permissionList = userConfig.getPermissios();
        if (permissionList != null) {
            listFields.put(UserConfig.PERMISSIONS_KEY, userConfig.getPermissios().stream()
                .map(e -> e.toString()).collect(Collectors.toList()));
        }

        // Serialize RLS filters as JSON and store in simple fields
        Map<String, List<String>> rlsFilters = userConfig.getRlsFilters();
        if (rlsFilters != null && !rlsFilters.isEmpty()) {
            simpleFields.put(UserConfig.RLS_FILTERS_KEY, JsonUtils.objectToString(rlsFilters));
        }

        ZNRecord znRecord = new ZNRecord(userConfig.getUserName());
        znRecord.setSimpleFields(simpleFields);
        znRecord.setListFields(listFields);
        return znRecord;
    }
}
