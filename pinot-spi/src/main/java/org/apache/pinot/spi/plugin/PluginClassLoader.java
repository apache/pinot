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

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.lang3.exception.ExceptionUtils;


public class PluginClassLoader extends URLClassLoader {

  private final ClassLoader classLoader;

  public PluginClassLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
    classLoader = PluginClassLoader.class.getClassLoader();
    Method method = null;
    try {
      method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);

    } catch (NoSuchMethodException e) {
      //this should never happen
      ExceptionUtils.rethrow(e);
    }
    method.setAccessible(true);
    for (URL url : urls) {
      try {
        method.invoke(classLoader, url);
      } catch (Exception e) {
        ExceptionUtils.rethrow(e);
      }
    }
  }

  @Override
  protected Class<?> loadClass(String name, boolean resolve)
      throws ClassNotFoundException {
    // has the class loaded already?
    Class<?> loadedClass = findLoadedClass(name);
    if (loadedClass == null) {
      try {
        if (classLoader != null) {
          loadedClass = classLoader.loadClass(name);
        }
      } catch (ClassNotFoundException ex) {
        // class not found in system class loader... silently skipping
      }

      try {
        // find the class from given jar urls as in first constructor parameter.
        if (loadedClass == null) {
          loadedClass = findClass(name);
        }
      } catch (ClassNotFoundException e) {
        // class is not found in the given urls.
        // Let's try it in parent classloader.
        // If class is still not found, then this method will throw class not found ex.
        loadedClass = super.loadClass(name, resolve);
      }
    }

    if (resolve) {      // marked to resolve
      resolveClass(loadedClass);
    }
    return loadedClass;
  }

  @Override
  public Enumeration<URL> getResources(String name)
      throws IOException {
    List<URL> allRes = new LinkedList<>();

    // load resources from sys class loader
    Enumeration<URL> sysResources = classLoader.getResources(name);
    if (sysResources != null) {
      while (sysResources.hasMoreElements()) {
        allRes.add(sysResources.nextElement());
      }
    }

    // load resource from this classloader
    Enumeration<URL> thisRes = findResources(name);
    if (thisRes != null) {
      while (thisRes.hasMoreElements()) {
        allRes.add(thisRes.nextElement());
      }
    }

    // then try finding resources from parent classloaders
    Enumeration<URL> parentRes = super.findResources(name);
    if (parentRes != null) {
      while (parentRes.hasMoreElements()) {
        allRes.add(parentRes.nextElement());
      }
    }

    return new Enumeration<URL>() {
      Iterator<URL> it = allRes.iterator();

      @Override
      public boolean hasMoreElements() {
        return it.hasNext();
      }

      @Override
      public URL nextElement() {
        return it.next();
      }
    };
  }

  @Override
  public URL getResource(String name) {
    URL res = null;
    if (classLoader != null) {
      res = classLoader.getResource(name);
    }
    if (res == null) {
      res = findResource(name);
    }
    if (res == null) {
      res = super.getResource(name);
    }
    return res;
  }
}
