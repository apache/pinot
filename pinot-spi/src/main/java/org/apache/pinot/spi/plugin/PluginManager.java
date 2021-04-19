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
package org.apache.pinot.spi.plugin;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PluginManager {

  public static final String PLUGINS_DIR_PROPERTY_NAME = "plugins.dir";
  public static final String PLUGINS_INCLUDE_PROPERTY_NAME = "plugins.include";
  public static final String DEFAULT_PLUGIN_NAME = "DEFAULT";
  private static final Logger LOGGER = LoggerFactory.getLogger(PluginManager.class);
  private static final String JAR_FILE_EXTENSION = "jar";
  private static PluginManager PLUGIN_MANAGER = new PluginManager();

  // For backward compatibility, this map holds a mapping from old plugins class name to its new class name.
  private static final Map<String, String> PLUGINS_BACKWARD_COMPATIBLE_CLASS_NAME_MAP = new HashMap<String, String>(){
    {
      // MessageDecoder
      put("org.apache.pinot.core.realtime.stream.SimpleAvroMessageDecoder", "org.apache.pinot.plugin.inputformat.avro.SimpleAvroMessageDecoder");
      put("org.apache.pinot.core.realtime.impl.kafka.KafkaAvroMessageDecoder", "org.apache.pinot.plugin.inputformat.avro.KafkaAvroMessageDecoder");
      put("org.apache.pinot.core.realtime.impl.kafka.KafkaJSONMessageDecoder", "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder");

      // RecordReader
      put("org.apache.pinot.core.data.readers.AvroRecordReader", "org.apache.pinot.plugin.inputformat.avro.AvroRecordReader");
      put("org.apache.pinot.core.data.readers.CSVRecordReader", "org.apache.pinot.plugin.inputformat.csv.CSVRecordReader");
      put("org.apache.pinot.core.data.readers.JSONRecordReader", "org.apache.pinot.plugin.inputformat.json.JSONRecordReader");
      put("org.apache.pinot.plugin.inputformat.json.JsonRecordReader", "org.apache.pinot.plugin.inputformat.json.JSONRecordReader");
      put("org.apache.pinot.orc.data.readers.ORCRecordReader", "org.apache.pinot.plugin.inputformat.orc.ORCRecordReader");
      put("org.apache.pinot.plugin.inputformat.orc.OrcRecordReader", "org.apache.pinot.plugin.inputformat.orc.ORCRecordReader");
      put("org.apache.pinot.parquet.data.readers.ParquetRecordReader", "org.apache.pinot.plugin.inputformat.parquet.ParquetRecordReader");
      put("org.apache.pinot.core.data.readers.ThriftRecordReader", "org.apache.pinot.plugin.inputformat.thrift.ThriftRecordReader");

      // PinotFS
      put("org.apache.pinot.filesystem.AzurePinotFS", "org.apache.pinot.plugin.filesystem.AzurePinotFS");
      put("org.apache.pinot.filesystem.HadoopPinotFS", "org.apache.pinot.plugin.filesystem.HadoopPinotFS");
      put("org.apache.pinot.filesystem.LocalPinotFS", "org.apache.pinot.spi.filesystem.LocalPinotFS");

      // StreamConsumerFactory
      put("org.apache.pinot.core.realtime.impl.kafka.KafkaConsumerFactory", "org.apache.pinot.plugin.stream.kafka09.KafkaConsumerFactory");
      put("org.apache.pinot.core.realtime.impl.kafka2.KafkaConsumerFactory", "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory");
    }
  };

  private static final Map<String, String> INPUT_FORMAT_TO_RECORD_READER_CLASS_NAME_MAP = new HashMap<String, String>(){
    {
      put("avro", "org.apache.pinot.plugin.inputformat.avro.AvroRecordReader");
      put("csv", "org.apache.pinot.plugin.inputformat.csv.CSVRecordReader");
      put("json", "org.apache.pinot.plugin.inputformat.json.JSONRecordReader");
      put("orc", "org.apache.pinot.plugin.inputformat.orc.ORCRecordReader");
      put("parquet", "org.apache.pinot.plugin.inputformat.parquet.ParquetRecordReader");
      put("protobuf", "org.apache.pinot.plugin.inputformat.protobuf.ProtoBufRecordReader");
      put("thrift", "org.apache.pinot.plugin.inputformat.thrift.ThriftRecordReader");
    }
  };

  private static final Map<String, String> INPUT_FORMAT_TO_RECORD_READER_CONFIG_CLASS_NAME_MAP = new HashMap<String, String>(){
    {
      put("csv", "org.apache.pinot.plugin.inputformat.csv.CSVRecordReaderConfig");
      put("protobuf", "org.apache.pinot.plugin.inputformat.protobuf.ProtoBufRecordReaderConfig");
      put("thrift", "org.apache.pinot.plugin.inputformat.thrift.ThriftRecordReaderConfig");
    }
  };

  private Map<Plugin, PluginClassLoader> _registry = new HashMap<>();
  private String _pluginsRootDir;
  private String _pluginsInclude;
  private boolean _initialized = false;

  private PluginManager() {
    _registry.put(new Plugin(DEFAULT_PLUGIN_NAME), createClassLoader(Collections.emptyList()));
    init();
  }

  public synchronized void init() {
    if (_initialized) {
      return;
    }
    try {
      _pluginsRootDir = System.getProperty(PLUGINS_DIR_PROPERTY_NAME);
    } catch (Exception e) {
      LOGGER.error("Failed to load env variable {}", PLUGINS_DIR_PROPERTY_NAME, e);
      _pluginsRootDir = null;
    }
    try {
      _pluginsInclude = System.getProperty(PLUGINS_INCLUDE_PROPERTY_NAME);
    } catch (Exception e) {
      LOGGER.error("Failed to load env variable {}", PLUGINS_INCLUDE_PROPERTY_NAME, e);
      _pluginsInclude = null;
    }
    init(_pluginsRootDir, _pluginsInclude);
    _initialized = true;
  }

  private void init(String pluginsRootDir, String pluginsInclude) {
    if (StringUtils.isEmpty(pluginsRootDir)) {
      LOGGER.info("Env variable '{}' is not specified. Set this env variable to load additional plugins.",
          PLUGINS_DIR_PROPERTY_NAME);
      return;
    } else {
      if (!new File(pluginsRootDir).exists()) {
        LOGGER.warn("Plugins root dir [{}] doesn't exist.", pluginsRootDir);
        return;
      }
      LOGGER.info("Plugins root dir is [{}]", pluginsRootDir);
    }
    Collection<File> jarFiles = FileUtils.listFiles(new File(pluginsRootDir), new String[]{JAR_FILE_EXTENSION}, true);
    List<String> pluginsToLoad = null;
    if (!StringUtils.isEmpty(pluginsInclude)) {
      pluginsToLoad = Arrays.asList(pluginsInclude.split(","));
      LOGGER.info("Trying to load plugins: [{}]", Arrays.toString(pluginsToLoad.toArray()));
    } else {
      LOGGER.info("Loading all plugins. Please use env variable '{}' to customize.", PLUGINS_INCLUDE_PROPERTY_NAME,
          Arrays.toString(jarFiles.toArray()));
    }
    for (File jarFile : jarFiles) {
      File pluginDir = jarFile.getParentFile();
      String pluginName = pluginDir.getName();
      if (pluginsToLoad != null) {
        if (!pluginsToLoad.contains(pluginName)) {
          continue;
        }
      }
      try {
        load(pluginName, pluginDir);
        LOGGER.info("Successfully Loaded plugin [{}] from dir [{}]", pluginName, pluginDir);
      } catch (Exception e) {
        LOGGER.error("Failed to load plugin [{}] from dir [{}]", pluginName, pluginDir, e);
      }
    }
    initRecordReaderClassMap();
  }

  private void initRecordReaderClassMap() {

  }

  /**
   * Loads jars recursively
   * @param pluginName
   * @param directory
   */
  public void load(String pluginName, File directory) {
    LOGGER.info("Trying to load plugin [{}] from location [{}]", pluginName, directory);
    Collection<File> jarFiles = FileUtils.listFiles(directory, new String[]{"jar"}, true);
    Collection<URL> urlList = new ArrayList<>();
    for (File jarFile : jarFiles) {
      try {
        urlList.add(jarFile.toURI().toURL());
        LOGGER.info("Successfully loaded plugin [{}] from jar file [{}]", pluginName, jarFile);
      } catch (MalformedURLException e) {
        LOGGER.error("Unable to load plugin [{}] jar file [{}]", pluginName, jarFile, e);
      }
    }
    PluginClassLoader classLoader = createClassLoader(urlList);
    _registry.put(new Plugin(pluginName), classLoader);
  }

  private PluginClassLoader createClassLoader(Collection<URL> urlList) {
    URL[] urls = new URL[urlList.size()];
    urlList.toArray(urls);
    //always sort to make the behavior predictable
    Arrays.sort(urls);
    return new PluginClassLoader(urls, this.getClass().getClassLoader());
  }

  /**
   * Loads a class. The class name can be in any of the following formats
   * <li>com.x.y.foo</li> loads the class in the default class path
   * <li>pluginName:com.x.y.foo</li> loads the class in plugin specific classloader
   * @param className
   * @return
   * @throws ClassNotFoundException
   */
  public Class<?> loadClass(String className)
      throws ClassNotFoundException {
    String pluginName = DEFAULT_PLUGIN_NAME;
    String realClassName = className;
    if (className.indexOf(":") > -1) {
      String[] split = className.split("\\:");
      pluginName = split[0];
      realClassName = split[1];
    }
    return loadClass(pluginName, realClassName);
  }

  /**
   * Loads a class using the plugin specific class loader
   * @param pluginName
   * @param className
   * @return
   * @throws ClassNotFoundException
   */
  public Class<?> loadClass(String pluginName, String className)
      throws ClassNotFoundException {
    // Backward compatible check.
    return _registry.get(new Plugin(pluginName)).loadClass(loadClassWithBackwardCompatibleCheck(className), true);
  }

  public static String loadClassWithBackwardCompatibleCheck(String className) {
    return PLUGINS_BACKWARD_COMPATIBLE_CLASS_NAME_MAP.getOrDefault(className, className);
  }

  /**
   * Create an instance of the className. The className can be in any of the following formats
   * <li>com.x.y.foo</li> loads the class in the default class path
   * <li>pluginName:com.x.y.foo</li> loads the class in plugin specific classloader
   * @param className
   * @return
   */
  public <T> T createInstance(String className)
      throws Exception {
    return createInstance(className, new Class[]{}, new Object[]{});
  }

  /**
   * Create an instance of the className. The className can be in any of the following formats
   * <li>com.x.y.foo</li> loads the class in the default class path
   * <li>pluginName:com.x.y.foo</li> loads the class in plugin specific classloader
   * @param className
   * @return
   */
  public <T> T createInstance(String className, Class[] argTypes, Object[] argValues)
      throws Exception {
    String pluginName = DEFAULT_PLUGIN_NAME;
    String realClassName = className;
    if (className.indexOf(":") > -1) {
      String[] split = className.split("\\:");
      pluginName = split[0];
      realClassName = split[1];
    }
    return createInstance(pluginName, realClassName, argTypes, argValues);
  }

  /**
   * Creates an instance of className using classloader specific to the plugin
   * @param pluginName
   * @param className
   * @param <T>
   * @return
   * @throws Exception
   */
  public <T> T createInstance(String pluginName, String className)
      throws Exception {
    return createInstance(pluginName, className, new Class[]{}, new Object[]{});
  }

  /**
   *
   * @param pluginName
   * @param className
   * @param argTypes
   * @param argValues
   * @param <T>
   * @return
   */
  public <T> T createInstance(String pluginName, String className, Class[] argTypes, Object[] argValues)
      throws Exception {
    PluginClassLoader pluginClassLoader = PLUGIN_MANAGER._registry.get(new Plugin(pluginName));
    try {
      Class<T> loadedClass = (Class<T>) pluginClassLoader.loadClass(loadClassWithBackwardCompatibleCheck(className), true);
      Constructor<?> constructor;
      constructor = loadedClass.getConstructor(argTypes);
      Object instance = constructor.newInstance(argValues);
      return (T) instance;
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException | ClassNotFoundException e) {
      throw e;
    }
  }

  public String getPluginsRootDir() {
    return _pluginsRootDir;
  }

  public static PluginManager get() {
    return PLUGIN_MANAGER;
  }

  public String getRecordReaderClassName(String inputFormat) {
    String inputFormatKey = inputFormat.toLowerCase();
    return INPUT_FORMAT_TO_RECORD_READER_CLASS_NAME_MAP.get(inputFormatKey);
  }

  public String getRecordReaderConfigClassName(String inputFormat) {
    String inputFormatKey = inputFormat.toLowerCase();
    return INPUT_FORMAT_TO_RECORD_READER_CONFIG_CLASS_NAME_MAP.get(inputFormatKey);
  }

  public void registerRecordReaderClass(String inputFormat, String recordReaderClass, String recordReaderConfigClass) {
    if (recordReaderClass != null) {
      INPUT_FORMAT_TO_RECORD_READER_CLASS_NAME_MAP.put(inputFormat.toLowerCase(), recordReaderClass);
    }
    if (recordReaderConfigClass != null) {
      INPUT_FORMAT_TO_RECORD_READER_CONFIG_CLASS_NAME_MAP.put(inputFormat.toLowerCase(), recordReaderConfigClass);
    }
  }
}
