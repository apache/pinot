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

import com.google.common.annotations.VisibleForTesting;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.plexus.classworlds.ClassWorld;
import org.codehaus.plexus.classworlds.realm.ClassRealm;
import org.codehaus.plexus.classworlds.realm.DuplicateRealmException;
import org.codehaus.plexus.classworlds.realm.NoSuchRealmException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PluginManager {

  public static final String PLUGINS_DIR_PROPERTY_NAME = "plugins.dir";
  public static final String PLUGINS_INCLUDE_PROPERTY_NAME = "plugins.include";
  public static final String DEFAULT_PLUGIN_NAME = "DEFAULT";
  private static final Logger LOGGER = LoggerFactory.getLogger(PluginManager.class);
  private static final String JAR_FILE_EXTENSION = "jar";
  private static final PluginManager PLUGIN_MANAGER = new PluginManager();
  private static final String PINOT_REALMID = "pinot";
  private static final String PINOUT_PLUGIN_PROPERTIES_FILE_NAME = "pinot-plugin.properties";

  // For backward compatibility, this map holds a mapping from old plugins class name to its new class name.
  private static final Map<String, String> PLUGINS_BACKWARD_COMPATIBLE_CLASS_NAME_MAP = new HashMap<String, String>() {
    {
      // MessageDecoder
      put("org.apache.pinot.core.realtime.stream.SimpleAvroMessageDecoder",
          "org.apache.pinot.plugin.inputformat.avro.SimpleAvroMessageDecoder");
      put("org.apache.pinot.core.realtime.impl.kafka.KafkaAvroMessageDecoder",
          "org.apache.pinot.plugin.inputformat.avro.KafkaAvroMessageDecoder");
      put("org.apache.pinot.core.realtime.impl.kafka.KafkaJSONMessageDecoder",
          "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder");

      // RecordReader
      put("org.apache.pinot.core.data.readers.AvroRecordReader",
          "org.apache.pinot.plugin.inputformat.avro.AvroRecordReader");
      put("org.apache.pinot.core.data.readers.CSVRecordReader",
          "org.apache.pinot.plugin.inputformat.csv.CSVRecordReader");
      put("org.apache.pinot.core.data.readers.JSONRecordReader",
          "org.apache.pinot.plugin.inputformat.json.JSONRecordReader");
      put("org.apache.pinot.plugin.inputformat.json.JsonRecordReader",
          "org.apache.pinot.plugin.inputformat.json.JSONRecordReader");
      put("org.apache.pinot.orc.data.readers.ORCRecordReader",
          "org.apache.pinot.plugin.inputformat.orc.ORCRecordReader");
      put("org.apache.pinot.plugin.inputformat.orc.OrcRecordReader",
          "org.apache.pinot.plugin.inputformat.orc.ORCRecordReader");
      put("org.apache.pinot.parquet.data.readers.ParquetRecordReader",
          "org.apache.pinot.plugin.inputformat.parquet.ParquetRecordReader");
      put("org.apache.pinot.core.data.readers.ThriftRecordReader",
          "org.apache.pinot.plugin.inputformat.thrift.ThriftRecordReader");

      // PinotFS
      put("org.apache.pinot.filesystem.AzurePinotFS", "org.apache.pinot.plugin.filesystem.AzurePinotFS");
      put("org.apache.pinot.filesystem.HadoopPinotFS", "org.apache.pinot.plugin.filesystem.HadoopPinotFS");
      put("org.apache.pinot.filesystem.LocalPinotFS", "org.apache.pinot.spi.filesystem.LocalPinotFS");

      // StreamConsumerFactory
      put("org.apache.pinot.core.realtime.impl.kafka2.KafkaConsumerFactory",
          "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory");
      put("org.apache.pinot.core.realtime.impl.kafka3.KafkaConsumerFactory",
          "org.apache.pinot.plugin.stream.kafka30.KafkaConsumerFactory");
    }
  };

  private static final Map<String, String> INPUT_FORMAT_TO_RECORD_READER_CLASS_NAME_MAP =
      new HashMap<String, String>() {
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

  private static final Map<String, String> INPUT_FORMAT_TO_RECORD_READER_CONFIG_CLASS_NAME_MAP =
      new HashMap<String, String>() {
        {
          put("avro", "org.apache.pinot.plugin.inputformat.avro.AvroRecordReaderConfig");
          put("csv", "org.apache.pinot.plugin.inputformat.csv.CSVRecordReaderConfig");
          put("protobuf", "org.apache.pinot.plugin.inputformat.protobuf.ProtoBufRecordReaderConfig");
          put("thrift", "org.apache.pinot.plugin.inputformat.thrift.ThriftRecordReaderConfig");
        }
      };

  private final ClassWorld _classWorld;
  private final Map<Plugin, PluginClassLoader> _registry;

  private String _pluginsDirectories;
  private String _pluginsInclude;
  private boolean _initialized = false;

  PluginManager() {
    // For the shaded plugins
    _registry = new HashMap<>();
    _registry.put(new Plugin(DEFAULT_PLUGIN_NAME), createClassLoader(Collections.emptyList()));

    // for the new pinot plugins
    try {
      _classWorld = new ClassWorld();
      // to simulate behavior of legacy code, however every plugin should have a dedicated realm
      _classWorld.newRealm(DEFAULT_PLUGIN_NAME);
      _classWorld.newRealm(PINOT_REALMID, ClassLoader.getSystemClassLoader());
    } catch (DuplicateRealmException e) {
      throw new RuntimeException(e);
    }

    init();
  }

  public synchronized void init() {
    if (_initialized) {
      return;
    }
    try {
      _pluginsDirectories = System.getProperty(PLUGINS_DIR_PROPERTY_NAME);
    } catch (Exception e) {
      LOGGER.error("Failed to load system property {}", PLUGINS_DIR_PROPERTY_NAME, e);
      _pluginsDirectories = null;
    }
    try {
      _pluginsInclude = System.getProperty(PLUGINS_INCLUDE_PROPERTY_NAME);
    } catch (Exception e) {
      LOGGER.error("Failed to load system property {}", PLUGINS_INCLUDE_PROPERTY_NAME, e);
      _pluginsInclude = null;
    }
    init(_pluginsDirectories, _pluginsInclude);
    _initialized = true;
  }

  private void init(String pluginsDirectories, String pluginsInclude) {
    if (StringUtils.isEmpty(pluginsDirectories)) {
      LOGGER.info("System property '{}' is not specified. Set this system property via the JVM arguments to load "
          + "additional plugins.", PLUGINS_DIR_PROPERTY_NAME);
    } else {
      try {
        HashMap<String, File> plugins = getPluginsToLoad(pluginsDirectories, pluginsInclude);
        LOGGER.info("#getPluginsToLoad has produced {} plugins to load", plugins.size());

        for (Map.Entry<String, File> entry : plugins.entrySet()) {
          String pluginName = entry.getKey();
          File pluginDir = entry.getValue();

          try {
            load(pluginName, pluginDir);
            LOGGER.info("Successfully Loaded plugin [{}] from dir [{}]", pluginName, pluginDir);
          } catch (Exception e) {
            LOGGER.error("Failed to load plugin [{}] from dir [{}]", pluginName, pluginDir, e);
          }
        }

        initRecordReaderClassMap();
      } catch (IllegalArgumentException e) {
        LOGGER.warn(e.getMessage());
      }
    }
  }

  /**
   * This method will take a semi-colon delimited string of directories and a semi-colon delimited string of plugin
   * names. It will traverse the directories in order and produce a <String, File> map of plugins to be loaded.
   * If a plugin is found in multiple directories, only the first copy of it will be picked up.
   * @param pluginsDirectories
   * @param pluginsInclude
   * @return A hash map with key = plugin name, value = file object
   */
  @VisibleForTesting
  public HashMap<String, File> getPluginsToLoad(String pluginsDirectories, String pluginsInclude)
      throws IllegalArgumentException {
    String[] directories = pluginsDirectories.split(";");
    LOGGER.info("Plugin directories: {}, parsed directories to load: '{}'", pluginsDirectories, directories);

    HashMap<String, File> finalPluginsToLoad = new HashMap<>();

    for (String pluginsDirectory : directories) {
      if (!new File(pluginsDirectory).exists()) {
        throw new IllegalArgumentException("Plugins dir [" + pluginsDirectory + "] doesn't exist.");
      }

      Collection<File> jarFiles =
          FileUtils.listFiles(new File(pluginsDirectory), new String[]{JAR_FILE_EXTENSION}, true);
      List<String> pluginsToLoad = null;
      if (!StringUtils.isEmpty(pluginsInclude)) {
        pluginsToLoad = Arrays.asList(pluginsInclude.split(";"));
        LOGGER.info("Potential plugins to load: [{}]", Arrays.toString(pluginsToLoad.toArray()));
      } else {
        LOGGER.info("Please use system property '{}' to customize plugins to load. Loading all plugins: {}",
            PLUGINS_INCLUDE_PROPERTY_NAME, Arrays.toString(jarFiles.toArray()));
      }

      for (File jarFile : jarFiles) {
        File pluginDir = jarFile.getParentFile();
        String pluginName = pluginDir.getName();
        LOGGER.info("Found plugin, pluginDir: {}, pluginName: {}", pluginDir, pluginName);
        if (pluginsToLoad != null) {
          if (!pluginsToLoad.contains(pluginName)) {
            LOGGER.info("Skipping plugin: {} is not inside pluginsToLoad {}", pluginName, pluginsToLoad);
            continue;
          }
        }

        if (!finalPluginsToLoad.containsKey(pluginName)) {
          finalPluginsToLoad.put(pluginName, pluginDir);
          LOGGER.info("Added [{}] from dir [{}] to final list of plugins to load", pluginName, pluginDir);
        }
      }
    }

    return finalPluginsToLoad;
  }

  private void initRecordReaderClassMap() {
  }

  /**
   * Loads jars recursively
   * @param pluginName
   * @param directory the directory of one plugin
   */
  public void load(String pluginName, File directory) {
    Path pluginPropertiesPath = directory.toPath().resolve(PINOUT_PLUGIN_PROPERTIES_FILE_NAME);
    if (Files.isRegularFile(pluginPropertiesPath)) {
      Properties pluginProperties = new Properties();
      PinotPluginConfiguration config;
      try (Reader reader = Files.newBufferedReader(pluginPropertiesPath)) {
        pluginProperties.load(reader);
        config = new PinotPluginConfiguration(pluginProperties);
      } catch (IOException e) {
        LOGGER.warn("Failed to load plugin properties from {}", pluginPropertiesPath, e);
        throw new UncheckedIOException(e);
      }

      final ClassLoader baseClassLoader = ClassLoader.getPlatformClassLoader();

      Collection<URL> urlList;
      try (Stream<Path> pluginClasspathEntries = Files.list(directory.toPath())) {
        urlList = pluginClasspathEntries.map(p -> {
          try {
            return p.toUri().toURL();
          } catch (MalformedURLException e) {
            throw new RuntimeException(e);
          }
        }).collect(Collectors.toList());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      try {
        ClassRealm pluginRealm = _classWorld.newRealm(pluginName, baseClassLoader);
        urlList.forEach(pluginRealm::addURL);

        ClassRealm pinotRealm = _classWorld.getClassRealm(PINOT_REALMID);

        // All packages to look up in pinot realm BEFORE itself
        Stream<String> importedPinotPackages =
            Stream.of("org.apache.pinot.spi"); // this works like a prefix, so ALL spi classes will be accessible
        importedPinotPackages.forEach(p -> pluginRealm.importFrom(pinotRealm, p));

        // Additional importForm as specified by the plugin configuration
        config.getImportsFromPerRealm().forEach((r, ifs) -> {
          try {
            ClassRealm cr = _classWorld.getRealm(r);
            ifs.forEach(i -> pluginRealm.importFrom(cr, i));
          } catch (NoSuchRealmException e) {
            LOGGER.warn("{} realm does not exist", r);
          }
        });

        // Important: parent is not the same as baseclassloader (see pluginRealm above)
        // baseClassLoader is BEFORE self classloader (should be Platform class loader)
        // parentClassLoader is AFTER self classloader
        config.getParentRealmId().map(_classWorld::getClassRealm).ifPresent(pluginRealm::setParentRealm);
      } catch (DuplicateRealmException e) {
        throw new RuntimeException(e);
      }
      LOGGER.info("Successfully loaded plugin [{}] from jar files: {}", pluginName, Arrays.toString(urlList.toArray()));
    } else {
      LOGGER.info("Trying to load plugin [{}] from location [{}]", pluginName, directory);
      Collection<File> jarFiles = FileUtils.listFiles(directory, new String[]{"jar"}, true);
      Collection<URL> urlList = new ArrayList<>();
      for (File jarFile : jarFiles) {
        try {
          urlList.add(jarFile.toURI().toURL());
        } catch (MalformedURLException e) {
          LOGGER.error("Unable to load plugin [{}] jar file [{}]", pluginName, jarFile, e);
        }
      }

      PluginClassLoader classLoader = createClassLoader(urlList);
      _registry.put(new Plugin(pluginName), classLoader);

      LOGGER.info("Successfully loaded plugin [{}] from jar files: {}", pluginName, Arrays.toString(urlList.toArray()));
    }
  }

  private PluginClassLoader createClassLoader(Collection<URL> urlList) {
    URL[] urls = new URL[urlList.size()];
    urlList.toArray(urls);
    //always sort to make the behavior predictable
    Arrays.sort(urls, Comparator.comparing(URL::toString));
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
    String name = loadClassWithBackwardCompatibleCheck(className);

    Plugin plugin = new Plugin(pluginName);
    if (_registry.containsKey(plugin)) {
      return _registry.get(plugin).loadClass(name, true);
    } else {
      try {
        return _classWorld.getRealm(pluginName).loadClass(className);
      } catch (NoSuchRealmException e) {
        throw new RuntimeException(e);
      }
    }
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
    Class<T> loadedClass;
    String name = loadClassWithBackwardCompatibleCheck(className);

    Plugin plugin = new Plugin(pluginName);
    if (_registry.containsKey(plugin)) {
      PluginClassLoader pluginClassLoader = _registry.get(plugin);
      loadedClass = (Class<T>) pluginClassLoader.loadClass(name, true);
    } else {
      loadedClass = (Class<T>) Class.forName(name, true, _classWorld.getRealm(pluginName));
    }
    Constructor<?> constructor;
    constructor = loadedClass.getConstructor(argTypes);
    Object instance = constructor.newInstance(argValues);
    return (T) instance;
  }

  public String[] getPluginsDirectories() {
    if (_pluginsDirectories != null) {
      return _pluginsDirectories.split(";");
    }
    return null;
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
