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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.io.FileUtils;


public class PluginManager {

  static PluginManager PLUGIN_MANAGER = new PluginManager();

  Map<Plugin, PluginClassLoader> _registry = new HashMap<>();

  private PluginManager() {

  }

  /**
   * Loads jars recursively
   * @param pluginName
   * @param directory
   */
  public void load(String pluginName, File directory) {
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
    return new PluginClassLoader(urls, Thread.currentThread().getContextClassLoader());
  }

  public Class<?> loadClass(String pluginName, String className)
      throws ClassNotFoundException {
    return PLUGIN_MANAGER._registry.get(new Plugin(pluginName)).loadClass(className, true);
  }

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
    Class<T> loadedClass = (Class<T>) pluginClassLoader.loadClass(className, true);
    Constructor<?> constructor = loadedClass.getConstructor(argTypes);
    if (constructor != null) {
      Object instance = constructor.newInstance(argValues);
      return (T) instance;
    }
    return null;
  }

  public static PluginManager get() {
    return PLUGIN_MANAGER;
  }
}

