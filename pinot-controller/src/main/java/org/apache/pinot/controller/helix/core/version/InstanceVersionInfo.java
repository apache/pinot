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
package org.apache.pinot.controller.helix.core.version;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;
import org.apache.pinot.common.version.ParsedVersion;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.CommonConstants.Helix;


/**
 * Version information for a single Pinot instance, derived from its Helix InstanceConfig.
 *
 * <p>The {@code rawVersion} field reflects what was written to the
 * {@link CommonConstants.Helix.Instance#PINOT_VERSION_KEY} field at the instance's last startup.
 * It may be {@code "UNKNOWN"} for instances that predate version reporting or that were built
 * outside a Maven environment.
 */
public class InstanceVersionInfo {

  @JsonProperty("instanceName")
  private final String _instanceName;

  @JsonProperty("componentType")
  private final String _componentType;

  @JsonProperty("rawVersion")
  private final String _rawVersion;

  @JsonProperty("isLive")
  private final boolean _isLive;

  // Excluded from JSON — used internally for comparisons
  private final transient ParsedVersion _parsedVersion;

  public InstanceVersionInfo(String instanceName, String componentType, String rawVersion,
      boolean isLive, @Nullable ParsedVersion parsedVersion) {
    _instanceName = instanceName;
    _componentType = componentType;
    _rawVersion = rawVersion;
    _isLive = isLive;
    _parsedVersion = parsedVersion;
  }

  public String getInstanceName() {
    return _instanceName;
  }

  public String getComponentType() {
    return _componentType;
  }

  public String getRawVersion() {
    return _rawVersion;
  }

  public boolean isLive() {
    return _isLive;
  }

  @JsonIgnore
  @Nullable
  public ParsedVersion getParsedVersion() {
    return _parsedVersion;
  }

  /** Returns {@code true} if the version was successfully parsed (not UNKNOWN or malformed). */
  @JsonIgnore
  public boolean isVersionKnown() {
    return _parsedVersion != null;
  }

  /**
   * Sentinel returned by {@link #componentTypeOf(String)} for instances whose prefix does not
   * match any of the known component kinds. Distinct from
   * {@link org.apache.pinot.common.version.PinotVersion#UNKNOWN} (which names an unparseable
   * <em>version</em>, not an instance kind).
   */
  public static final String UNKNOWN_TYPE = "UNKNOWN_TYPE";

  /**
   * Derives the component type from an instance name prefix.  Returns {@link #UNKNOWN_TYPE} for
   * any instance name that does not match one of the known prefixes.
   *
   * <p>Note: this is stricter than {@link org.apache.pinot.spi.utils.InstanceTypeUtils#isServer}
   * which treats unprefixed names as servers. For version-compatibility reporting we prefer to
   * surface unknown instance kinds explicitly rather than silently bucketing them as SERVER.
   */
  public static String componentTypeOf(String instanceName) {
    if (instanceName.startsWith(Helix.PREFIX_OF_CONTROLLER_INSTANCE)) {
      return "CONTROLLER";
    }
    if (instanceName.startsWith(Helix.PREFIX_OF_BROKER_INSTANCE)) {
      return "BROKER";
    }
    if (instanceName.startsWith(Helix.PREFIX_OF_SERVER_INSTANCE)) {
      return "SERVER";
    }
    if (instanceName.startsWith(Helix.PREFIX_OF_MINION_INSTANCE)) {
      return "MINION";
    }
    return UNKNOWN_TYPE;
  }
}
