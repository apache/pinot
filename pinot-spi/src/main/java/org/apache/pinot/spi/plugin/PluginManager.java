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

  private static final Logger LOGGER = LoggerFactory.getLogger(PluginManager.class);
  public static final String PLUGINS_DIR_ENV_VAR = "plugins.dir";
  public static final String PLUGINS_INCLUDE_ENV_VAR = "plugins.include";
  public static final String DEFAULT_PLUGIN_NAME = "DEFAULT";
  private static final String JAR_FILE_EXTENSION = "jar";
  static PluginManager PLUGIN_MANAGER = new PluginManager();

  Map<Plugin, PluginClassLoader> _registry = new HashMap<>();
  String pluginsRootDir;
  String pluginsInclude;

  private PluginManager() {
    _registry.put(new Plugin(DEFAULT_PLUGIN_NAME), createClassLoader(Collections.emptyList()));
    try {
      pluginsRootDir = System.getProperty(PLUGINS_DIR_ENV_VAR);
    } catch (Exception e) {
      LOGGER.error("Failed to load env variable {}", PLUGINS_DIR_ENV_VAR, e);
      pluginsRootDir = null;
    }
    try {
      pluginsInclude = System.getProperty(PLUGINS_INCLUDE_ENV_VAR);
    } catch (Exception e) {
      LOGGER.error("Failed to load env variable {}", PLUGINS_INCLUDE_ENV_VAR, e);
      pluginsInclude = null;
    }
    init(pluginsRootDir, pluginsInclude);
    printLoadedPlugins();
  }

  public void printLoadedPlugins() {
    for (Plugin plugin : _registry.keySet()) {
      PluginClassLoader pluginClassLoader = _registry.get(plugin);
      LOGGER.info("Loaded plugin : {}, with classloader: {}", plugin, pluginClassLoader);
    }
  }

  private void init(String pluginsRootDir, String pluginsLoading) {
    if (StringUtils.isEmpty(pluginsRootDir)) {
      LOGGER.info("Env variable '{}' is not specified. Set this env variable to load additional plugins.",
          PLUGINS_DIR_ENV_VAR);
      return;
    } else {
      LOGGER.info("Plugins root dir is [{}]", pluginsRootDir);
    }
    Collection<File> jarFiles = FileUtils.listFiles(new File(pluginsRootDir), new String[]{JAR_FILE_EXTENSION}, true);
    List<String> pluginsToLoad = null;
    if (!StringUtils.isEmpty(pluginsLoading)) {
      pluginsToLoad = Arrays.asList(pluginsLoading.split(","));
      LOGGER.info("Trying to load plugins: [{}]", Arrays.toString(pluginsToLoad.toArray()));
    } else {
      LOGGER.info("Loading all plugins. Please use env variable '{}' to customize.", PLUGINS_INCLUDE_ENV_VAR, Arrays.toString(jarFiles.toArray()));
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
        LOGGER.info("Successfully Loaded plugin [{}] from dir [{}]",pluginName , pluginDir);
      } catch (Exception e) {
        LOGGER.error("Failed to load plugin [{}] from dir [{}]",pluginName , pluginDir, e);
      }
    }
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
      } catch (MalformedURLException e) {
        //ignore
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
    return _registry.get(new Plugin(pluginName)).loadClass(className, true);
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
      Class<T> loadedClass = (Class<T>) pluginClassLoader.loadClass(className, true);
      Constructor<?> constructor;
      constructor = loadedClass.getConstructor(argTypes);
      Object instance = constructor.newInstance(argValues);
      return (T) instance;
    } catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException | ClassNotFoundException e) {
      throw e;
    }
  }

  public static PluginManager get() {
    return PLUGIN_MANAGER;
  }
}

