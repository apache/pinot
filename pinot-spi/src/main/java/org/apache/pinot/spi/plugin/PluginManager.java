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
import java.io.InputStream;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
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
      // Old-style class names mapped to current plugin packages
      // Rename the usage of kafka 2 to kafka 3
      put("org.apache.pinot.core.realtime.impl.kafka2.KafkaConsumerFactory",
          "org.apache.pinot.plugin.stream.kafka30.KafkaConsumerFactory");
      put("org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory",
          "org.apache.pinot.plugin.stream.kafka30.KafkaConsumerFactory");
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
          put("parquet", "org.apache.pinot.plugin.inputformat.parquet.ParquetRecordReaderConfig");
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
    PinotPluginConfiguration config = readPluginConfiguration(directory);
    if (config != null) {
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

  /// Reads {@code pinot-plugin.properties} from the plugin directory.
  ///
  /// Looks first for the file at the directory root (`<dir>/pinot-plugin.properties`) — the
  /// historical layout — and falls back to scanning the jars in the directory for the same
  /// resource. The fallback lets a plugin author opt into realm loading by simply placing the
  /// file under `src/main/resources` of the plugin module: at runtime the file ends up inside
  /// the plugin jar, and PluginManager finds it without requiring the distribution assembly to
  /// extract it next to the jar.
  ///
  /// Returns {@code null} when neither lookup turns up the file — the caller falls back to the
  /// legacy {@link PluginClassLoader} path.
  private PinotPluginConfiguration readPluginConfiguration(File directory) {
    Path pluginPropertiesPath = directory.toPath().resolve(PINOUT_PLUGIN_PROPERTIES_FILE_NAME);
    if (Files.isRegularFile(pluginPropertiesPath)) {
      Properties pluginProperties = new Properties();
      try (Reader reader = Files.newBufferedReader(pluginPropertiesPath)) {
        pluginProperties.load(reader);
        return new PinotPluginConfiguration(pluginProperties);
      } catch (IOException e) {
        LOGGER.warn("Failed to load plugin properties from {}", pluginPropertiesPath, e);
        throw new UncheckedIOException(e);
      }
    }
    // Fall back to scanning jars in the plugin directory for the same resource. Plugin authors
    // who opt into realm loading by adding `src/main/resources/pinot-plugin.properties` get the
    // file inside their plugin jar — without this lookup, that placement would be ignored.
    //
    // Order the scan so the "obvious" jar (the one whose filename starts with the plugin
    // directory name — e.g. `pinot-kafka-3.0/pinot-kafka-3.0-VERSION-shaded.jar`) is inspected
    // first, then the rest. Avoids opening every dependency jar in the directory in the common
    // case, which can be 100+ jars per plugin.
    String prefix = directory.getName();
    Collection<File> jarFiles = FileUtils.listFiles(directory, new String[]{JAR_FILE_EXTENSION}, true);
    List<File> ordered = new ArrayList<>(jarFiles.size());
    List<File> tail = new ArrayList<>();
    for (File jarFile : jarFiles) {
      if (jarFile.getName().startsWith(prefix)) {
        ordered.add(jarFile);
      } else {
        tail.add(jarFile);
      }
    }
    ordered.addAll(tail);
    for (File jarFile : ordered) {
      PinotPluginConfiguration config = readPluginConfigurationFromJar(jarFile);
      if (config != null) {
        return config;
      }
    }
    return null;
  }

  private PinotPluginConfiguration readPluginConfigurationFromJar(File jarFile) {
    try (JarFile jar = new JarFile(jarFile)) {
      JarEntry entry = jar.getJarEntry(PINOUT_PLUGIN_PROPERTIES_FILE_NAME);
      if (entry == null) {
        return null;
      }
      Properties pluginProperties = new Properties();
      try (InputStream in = jar.getInputStream(entry)) {
        pluginProperties.load(in);
        LOGGER.info("Loaded plugin properties from {} entry of {}", PINOUT_PLUGIN_PROPERTIES_FILE_NAME, jarFile);
        return new PinotPluginConfiguration(pluginProperties);
      }
    } catch (IOException e) {
      // Don't throw: a corrupt jar should not prevent us from inspecting the rest of the
      // plugin directory. Worst case we miss the properties file and the caller falls back to
      // the legacy PluginClassLoader path; the WARN message is the operator's signal.
      LOGGER.warn("Could not read {} from {} — falling back to legacy plugin loading if no other jar declares it.",
          PINOUT_PLUGIN_PROPERTIES_FILE_NAME, jarFile, e);
      return null;
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
   * Loads a class. The class name can be in any of the following formats:
   * <ul>
   *   <li>{@code com.x.y.Foo} — load via the realm-walk fallback: try the default
   *       PluginClassLoader (which delegates to the system classloader) first, then every
   *       registered plugin realm and PluginClassLoader. The first match wins; if more than
   *       one plugin exposes the same FQCN, a WARN is logged.</li>
   *   <li>{@code pluginName:com.x.y.Foo} — strict load via the named plugin's classloader
   *       only. If that plugin does not have the class, throws {@link ClassNotFoundException}
   *       without falling through to other plugins.</li>
   * </ul>
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
   * Loads a class using the plugin-specific class loader for the given {@code pluginName}, or, when
   * {@code pluginName} is {@link #DEFAULT_PLUGIN_NAME}, walks every plugin classloader after
   * exhausting the default one. The realm walk lets callers look up a class by FQCN without
   * having to know which plugin provides it — important once plugin classes are no longer
   * unconditionally on the JVM's system classpath.
   *
   * <p>For an explicit (non-{@code DEFAULT}) {@code pluginName}, the lookup is strict: only
   * that plugin's classloader / realm is consulted, so a {@code "pluginA:Foo"} request never
   * silently resolves to a {@code Foo} from {@code pluginB}.</p>
   *
   * @param pluginName the plugin to load from, or {@link #DEFAULT_PLUGIN_NAME} for an
   *     unscoped lookup that walks every registered plugin classloader.
   * @param className FQCN to load; subject to backward-compatible class-name remapping.
   * @throws ClassNotFoundException if the class is not found on the named plugin (or, for
   *     {@code DEFAULT}, on any classloader known to this manager).
   */
  public Class<?> loadClass(String pluginName, String className)
      throws ClassNotFoundException {
    String name = loadClassWithBackwardCompatibleCheck(className);
    if (DEFAULT_PLUGIN_NAME.equals(pluginName)) {
      return loadClassFromAnyPlugin(name);
    }
    Plugin plugin = new Plugin(pluginName);
    if (_registry.containsKey(plugin)) {
      return _registry.get(plugin).loadClass(name, true);
    }
    try {
      return _classWorld.getRealm(pluginName).loadClass(name);
    } catch (NoSuchRealmException e) {
      throw new RuntimeException(e);
    }
  }

  /// Walk-all-plugins fallback used when the caller did not pin a specific {@code pluginName}.
  ///
  /// Order is deterministic: the `DEFAULT` `PluginClassLoader` first (its parent is the system
  /// classloader, so this also covers core service classes), then every [ClassRealm] in
  /// `ClassWorld` registration order, then every other entry in the legacy `PluginClassLoader`
  /// registry in registration order. Returns the **first** match.
  ///
  /// If more than one classloader exposes the same FQCN — e.g. two plugins ship a class with
  /// the same name, or a relocated dependency leaks the same package — the first hit wins and
  /// a `WARN` is logged listing every classloader that also resolved the name. Callers that
  /// need strict precedence must use the explicit `pluginName:className` form.
  private Class<?> loadClassFromAnyPlugin(String name) throws ClassNotFoundException {
    List<ClassLoader> classLoaders;
    synchronized (this) {
      classLoaders = new ArrayList<>(1 + _classWorld.getRealms().size() + _registry.size());
      Plugin defaultPlugin = new Plugin(DEFAULT_PLUGIN_NAME);
      PluginClassLoader defaultClassLoader = _registry.get(defaultPlugin);
      if (defaultClassLoader != null) {
        classLoaders.add(defaultClassLoader);
      }
      // PINOT_REALMID wraps the system classloader (already covered by the DEFAULT
      // PluginClassLoader's parent delegation); DEFAULT_PLUGIN_NAME has no URLs.
      for (ClassRealm realm : _classWorld.getRealms()) {
        if (PINOT_REALMID.equals(realm.getId()) || DEFAULT_PLUGIN_NAME.equals(realm.getId())) {
          continue;
        }
        classLoaders.add(realm);
      }
      for (Map.Entry<Plugin, PluginClassLoader> entry : _registry.entrySet()) {
        if (defaultPlugin.equals(entry.getKey())) {
          continue;
        }
        classLoaders.add(entry.getValue());
      }
    }
    Class<?> firstHit = null;
    ClassLoader firstHitLoader = null;
    List<ClassLoader> additionalHitLoaders = null;
    for (ClassLoader cl : classLoaders) {
      try {
        Class<?> c = Class.forName(name, true, cl);
        if (firstHit == null) {
          firstHit = c;
          firstHitLoader = cl;
        } else {
          if (additionalHitLoaders == null) {
            additionalHitLoaders = new ArrayList<>(2);
          }
          additionalHitLoaders.add(cl);
        }
      } catch (ClassNotFoundException ignored) {
      } catch (NoClassDefFoundError e) {
        // The class bytecode was found on this classloader but a dependency class could not be
        // resolved (e.g. the plugin jar is on the system classpath but its native deps are not).
        // Continue walking — a later classloader (e.g. the plugin's own realm) may have all deps.
        LOGGER.debug("Skipping classloader {} for class {}: {}", cl, name, e.toString());
      }
    }
    if (firstHit == null) {
      throw new ClassNotFoundException(name);
    }
    if (additionalHitLoaders != null) {
      LOGGER.warn("Class {} resolved on multiple plugin classloaders; using {} (also found on {}). "
              + "Use pluginName:className to disambiguate.", name, firstHitLoader, additionalHitLoaders);
    }
    return firstHit;
  }

  public static String loadClassWithBackwardCompatibleCheck(String className) {
    return PLUGINS_BACKWARD_COMPATIBLE_CLASS_NAME_MAP.getOrDefault(className, className);
  }

  /**
   * Create an instance of the className. See {@link #loadClass(String)} for the resolution
   * rules — bare FQCNs walk every plugin classloader; the {@code pluginName:FQCN} form pins
   * the lookup to that plugin.
   * @param className
   * @return
   */
  public <T> T createInstance(String className)
      throws Exception {
    return createInstance(className, new Class[]{}, new Object[]{});
  }

  /**
   * Create an instance of the className. See {@link #loadClass(String)} for the resolution
   * rules — bare FQCNs walk every plugin classloader; the {@code pluginName:FQCN} form pins
   * the lookup to that plugin.
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
    // Reuse loadClass so DEFAULT_PLUGIN_NAME triggers the realm walk and explicit pluginNames
    // stay strict. Previously this method bypassed loadClass and only consulted the named
    // plugin, which meant `createInstance(className)` (no prefix) only saw classes on the
    // system classloader — fine while plugins were unconditionally on the JVM classpath, broken
    // once they live in isolated realms.
    Class<T> loadedClass = (Class<T>) loadClass(pluginName, className);
    Constructor<?> constructor = loadedClass.getConstructor(argTypes);
    Object instance = constructor.newInstance(argValues);
    return (T) instance;
  }

  public String[] getPluginsDirectories() {
    if (_pluginsDirectories != null) {
      return _pluginsDirectories.split(";");
    }
    return null;
  }

  /// Discovers all `ServiceLoader` implementations of `iface` across every plugin classloader
  /// known to this manager (both legacy [PluginClassLoader] entries and Plexus
  /// [ClassRealm]s) plus the classloader that loaded this class.
  ///
  /// Use this in place of `ServiceLoader.load(iface)` (which only consults the
  /// thread-context classloader). When plugins live in isolated realms — i.e. their classes
  /// are not visible to the system classloader — a plain `ServiceLoader.load(iface)` will
  /// silently miss plugin-provided implementations.
  ///
  /// Implementations are de-duplicated by fully-qualified class name, so the same impl class
  /// reachable via two classloaders is returned once; the first one wins. Iteration order is:
  /// this manager's classloader first, then realms in `ClassWorld` registration order, then
  /// legacy `PluginClassLoader` registry order. A malformed `META-INF/services` resource on a
  /// single classloader logs a warning and is skipped — it does not break discovery for other
  /// classloaders.
  ///
  /// Each invocation performs a fresh walk; results are not cached. The classloader list is
  /// snapshotted under synchronisation so a concurrent [#load] does not produce a
  /// `ConcurrentModificationException`, but the snapshot is point-in-time: a plugin
  /// registered after the snapshot is taken won't be visible to that call. Safe to call from
  /// any thread under those semantics.
  public <T> List<T> loadServices(Class<T> iface) {
    List<ClassLoader> classLoaders;
    synchronized (this) {
      classLoaders = new ArrayList<>(_classWorld.getRealms().size() + _registry.size());
      classLoaders.add(getClass().getClassLoader());
      for (ClassRealm realm : _classWorld.getRealms()) {
        classLoaders.add(realm);
      }
      for (PluginClassLoader pluginClassLoader : _registry.values()) {
        classLoaders.add(pluginClassLoader);
      }
    }
    List<T> results = new ArrayList<>();
    Set<String> seenFqcns = new HashSet<>();
    for (ClassLoader cl : classLoaders) {
      loadServicesInto(iface, cl, results, seenFqcns);
    }
    return results;
  }

  private static <T> void loadServicesInto(Class<T> iface, ClassLoader classLoader, List<T> results,
      Set<String> seenFqcns) {
    Iterator<T> iterator;
    try {
      iterator = ServiceLoader.load(iface, classLoader).iterator();
    } catch (ServiceConfigurationError e) {
      LOGGER.warn("Failed to start ServiceLoader for {} from classloader {}", iface.getName(), classLoader, e);
      return;
    }
    // ServiceLoader can throw ServiceConfigurationError from both `hasNext()` and `next()` —
    // skip the malformed entry and keep iterating. The iterator advances internally on error,
    // so retrying `hasNext()` lets us recover without losing well-formed entries.
    while (true) {
      boolean hasNext;
      try {
        hasNext = iterator.hasNext();
      } catch (ServiceConfigurationError e) {
        LOGGER.warn("Skipping malformed service entry for {} on classloader {}", iface.getName(), classLoader, e);
        continue;
      }
      if (!hasNext) {
        return;
      }
      T impl;
      try {
        impl = iterator.next();
      } catch (ServiceConfigurationError e) {
        LOGGER.warn("Skipping unloadable service for {} on classloader {}", iface.getName(), classLoader, e);
        continue;
      }
      if (seenFqcns.add(impl.getClass().getName())) {
        results.add(impl);
      }
    }
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

  // Register plugin class names when users try to deprecate old plugins/libraries.
  public void registerBackwardCompatibleClassName(String oldClassName, String newClassName) {
    String existingNewClassName = PLUGINS_BACKWARD_COMPATIBLE_CLASS_NAME_MAP.put(oldClassName, newClassName);
    if (existingNewClassName != null) {
      LOGGER.warn("There is already a mapping for backward compatible class from old [{}] to [{}], override it to [{}]",
          oldClassName, existingNewClassName, newClassName);
    } else {
      LOGGER.info("Registered backward compatible class name mapping from old [{}] to new [{}]", oldClassName,
          newClassName);
    }
  }
}
