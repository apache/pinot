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
package org.apache.pinot.common.utils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryManagerMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.env.PinotConfiguration;


/**
 * Class that represents various configs for a pinot component:
 * <ul>
 *   <li>System Configs</li>
 *   <li>JVM Configs</li>
 *   <li>Runtime Configs</li>
 *   <li>PinotConfiguration</li>
 * </ul>
 *
 * This class is JSON serializable and de-serializable.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({"systemConfig", "runtimeConfig", "pinotConfig", "jvmConfig"})
public class PinotAppConfigs {
  private static final String UNKNOWN_VALUE = "-";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @JsonProperty("systemConfig")
  private SystemConfig _systemConfig;

  @JsonProperty("jvmConfig")
  private JVMConfig _jvmConfig;

  @JsonProperty("runtimeConfig")
  private RuntimeConfig _runtimeConfig;

  @JsonProperty("pinotConfig")
  private Map<String, Object> _pinotConfig;

  @JsonCreator
  @SuppressWarnings("unused")
  public PinotAppConfigs() {

  }

  public PinotAppConfigs(PinotConfiguration pinotConfig) {
    _systemConfig = buildSystemConfig();
    _jvmConfig = buildJVMConfig();
    _runtimeConfig = buildRuntimeConfig();
    _pinotConfig = pinotConfig.toMap();
  }

  public SystemConfig getSystemConfig() {
    return _systemConfig;
  }

  public JVMConfig getJvmConfig() {
    return _jvmConfig;
  }

  public RuntimeConfig getRuntimeConfig() {
    return _runtimeConfig;
  }

  public Map<String, Object> getPinotConfig() {
    return _pinotConfig;
  }

  private SystemConfig buildSystemConfig() {
    OperatingSystemMXBean osMXBean = ManagementFactory.getOperatingSystemMXBean();
    SystemConfig systemConfig;

    // Not all platforms may implement this, and so we may not have access to some of the api's.
    if (osMXBean instanceof com.sun.management.OperatingSystemMXBean) {
      com.sun.management.OperatingSystemMXBean sunOsMXBean = (com.sun.management.OperatingSystemMXBean) osMXBean;
      systemConfig = new SystemConfig(osMXBean.getArch(), osMXBean.getName(), osMXBean.getVersion(), osMXBean.getAvailableProcessors(),
          FileUtils.byteCountToDisplaySize(sunOsMXBean.getTotalPhysicalMemorySize()),
          FileUtils.byteCountToDisplaySize(sunOsMXBean.getFreePhysicalMemorySize()),
          FileUtils.byteCountToDisplaySize(sunOsMXBean.getTotalSwapSpaceSize()),
          FileUtils.byteCountToDisplaySize(sunOsMXBean.getFreeSwapSpaceSize()));
    } else {
      systemConfig =
          new SystemConfig(osMXBean.getArch(), osMXBean.getName(), osMXBean.getVersion(), osMXBean.getAvailableProcessors(), UNKNOWN_VALUE,
              UNKNOWN_VALUE, UNKNOWN_VALUE, UNKNOWN_VALUE);
    }
    return systemConfig;
  }

  private JVMConfig buildJVMConfig() {
    RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    List<GarbageCollectorMXBean> garbageCollectorMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
    String bootClassPath = runtimeMXBean.isBootClassPathSupported() ? runtimeMXBean.getBootClassPath() : null;
    return new JVMConfig(runtimeMXBean.getInputArguments(), runtimeMXBean.getLibraryPath(), bootClassPath,
        runtimeMXBean.getSystemProperties(), System.getenv(),
        garbageCollectorMXBeans.stream().map(MemoryManagerMXBean::getName).collect(Collectors.toList()));
  }

  private RuntimeConfig buildRuntimeConfig() {
    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();

    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    return new RuntimeConfig(threadMXBean.getTotalStartedThreadCount(), threadMXBean.getThreadCount(),
        FileUtils.byteCountToDisplaySize(heapMemoryUsage.getMax()), FileUtils.byteCountToDisplaySize(heapMemoryUsage.getUsed()));
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class SystemConfig {
    private final String _arch;
    private final String _name;
    private final String _version;
    private final int _availableProcessors;
    private final String _totalPhysicalMemory;
    private final String _freePhysicalMemory;
    private final String _totalSwapSpace;
    private final String _freeSwapSpace;

    @JsonCreator
    public SystemConfig(@JsonProperty("arch") String arch, @JsonProperty("name") String name, @JsonProperty("version") String version,
        @JsonProperty("availableProcessors") int availableProcessors, @JsonProperty("totalPhysicalMemory") String totalPhysicalMemory,
        @JsonProperty("freePhysicalMemory") String freePhysicalMemory, @JsonProperty("totalSwapSpace") String totalSwapSpace,
        @JsonProperty("freeSwapSpace") String freeSwapSpace) {
      _arch = arch;
      _name = name;
      _version = version;
      _availableProcessors = availableProcessors;
      _totalPhysicalMemory = totalPhysicalMemory;
      _freePhysicalMemory = freePhysicalMemory;
      _totalSwapSpace = totalSwapSpace;
      _freeSwapSpace = freeSwapSpace;
    }

    public String getArch() {
      return _arch;
    }

    public String getName() {
      return _name;
    }

    public String getVersion() {
      return _version;
    }

    public int getAvailableProcessors() {
      return _availableProcessors;
    }

    public String getTotalPhysicalMemory() {
      return _totalPhysicalMemory;
    }

    public String getFreePhysicalMemory() {
      return _freePhysicalMemory;
    }

    public String getTotalSwapSpace() {
      return _totalSwapSpace;
    }

    public String getFreeSwapSpace() {
      return _freeSwapSpace;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SystemConfig that = (SystemConfig) o;
      return _availableProcessors == that._availableProcessors && Objects.equals(_arch, that._arch) && Objects.equals(_name, that._name)
          && Objects.equals(_version, that._version) && Objects.equals(_totalPhysicalMemory, that._totalPhysicalMemory) && Objects
          .equals(_freePhysicalMemory, that._freePhysicalMemory) && Objects.equals(_totalSwapSpace, that._totalSwapSpace) && Objects
          .equals(_freeSwapSpace, that._freeSwapSpace);
    }

    @Override
    public int hashCode() {
      return Objects
          .hash(_arch, _name, _version, _availableProcessors, _totalPhysicalMemory, _freePhysicalMemory, _totalSwapSpace, _freeSwapSpace);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class RuntimeConfig {
    private final long _numTotalThreads;
    private final int _numCurrentThreads;
    private final String _maxHeapSize;
    private final String _currentHeapSize;

    @JsonCreator
    public RuntimeConfig(@JsonProperty("numTotalThreads") long numTotalThreads, @JsonProperty("numCurrentThreads") int numCurrentThreads,
        @JsonProperty("maxHeapSize") String maxHeapSize, @JsonProperty("currentHeapSize") String currentHeapSize) {
      _numTotalThreads = numTotalThreads;
      _numCurrentThreads = numCurrentThreads;
      _maxHeapSize = maxHeapSize;
      _currentHeapSize = currentHeapSize;
    }

    public long getNumTotalThreads() {
      return _numTotalThreads;
    }

    public int getNumCurrentThreads() {
      return _numCurrentThreads;
    }

    public String getMaxHeapSize() {
      return _maxHeapSize;
    }

    public String getCurrentHeapSize() {
      return _currentHeapSize;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RuntimeConfig that = (RuntimeConfig) o;
      return _numTotalThreads == that._numTotalThreads && _numCurrentThreads == that._numCurrentThreads && Objects
          .equals(_maxHeapSize, that._maxHeapSize) && Objects.equals(_currentHeapSize, that._currentHeapSize);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_numTotalThreads, _numCurrentThreads, _maxHeapSize, _currentHeapSize);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPropertyOrder({"args", "garbageCollectors", "envVariables", "libraryPath", "bootClassPath"})
  public static class JVMConfig {
    private final List<String> _args;
    private final String _libraryPath;
    private final String _bootClassPath;
    private final Map<String, String> _envVariables;
    private final Map<String, String> _systemProperties;
    private final List<String> _garbageCollectors;

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      JVMConfig jvmConfig = (JVMConfig) o;
      return Objects.equals(_args, jvmConfig._args) && Objects.equals(_libraryPath, jvmConfig._libraryPath) && Objects
          .equals(_bootClassPath, jvmConfig._bootClassPath) && Objects.equals(_envVariables, jvmConfig._envVariables) && Objects
          .equals(_systemProperties, jvmConfig._systemProperties) && Objects.equals(_garbageCollectors, jvmConfig._garbageCollectors);
    }

    @Override
    public int hashCode() {
      return Objects.hash(_args, _libraryPath, _bootClassPath, _envVariables, _systemProperties, _garbageCollectors);
    }

    @JsonCreator
    public JVMConfig(@JsonProperty("args") List<String> args, @JsonProperty("libraryPath") String libraryPath,
        @JsonProperty("bootClassPath") String bootClassPath, @JsonProperty("systemProperties") Map<String, String> systemProperties,
        @JsonProperty("envVariables") Map<String, String> envVariables, @JsonProperty("garbageCollectors") List<String> garbageCollectors) {
      _args = args;
      _libraryPath = libraryPath;
      _bootClassPath = bootClassPath;
      _envVariables = envVariables;
      _systemProperties = systemProperties;
      _garbageCollectors = garbageCollectors;
    }

    public List<String> getArgs() {
      return _args;
    }

    public String getLibraryPath() {
      return _libraryPath;
    }

    public String getBootClassPath() {
      return _bootClassPath;
    }

    public Map<String, String> getEnvVariables() {
      return _envVariables;
    }

    public Map<String, String> getSystemProperties() {
      return _systemProperties;
    }

    public List<String> getGarbageCollectors() {
      return _garbageCollectors;
    }
  }

  public String toJSONString() {
    try {
      return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(this);
    } catch (JsonProcessingException e) {
      return e.getMessage();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PinotAppConfigs that = (PinotAppConfigs) o;
    return Objects.equals(_systemConfig, that._systemConfig) && Objects.equals(_jvmConfig, that._jvmConfig) && Objects
        .equals(_runtimeConfig, that._runtimeConfig) && Objects.equals(_pinotConfig, that._pinotConfig);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_systemConfig, _jvmConfig, _runtimeConfig, _pinotConfig);
  }
}
