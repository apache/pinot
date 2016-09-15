/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.utils;

import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * DataTableSerDe Registry singleton.
 *
 */
@ThreadSafe
public class DataTableSerDeRegistry {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataTableSerDeRegistry.class);
  private static DataTableSerDeRegistry _instance = new DataTableSerDeRegistry();

  DataTableSerDe _dataTableSerDe;
  DataTableSerDe _defaultDataTableSerDe;
  boolean _registered;

  public static DataTableSerDeRegistry getInstance() {
    return _instance;
  }

  /**
   * Private constructor for singleton.
   */
  private DataTableSerDeRegistry() {
    _dataTableSerDe = null;
    _defaultDataTableSerDe = new DataTableJavaSerDe();
    _registered = false;
  }

  /**
   * Registers the provided DataTableSerDe instance if none was registered yet.
   * Throws {@link UnsupportedOperationException} if one was registered already.
   *
   * @param dataTableSerDe DataTableSerDe instance to register.
   */
  public synchronized void register(DataTableSerDe dataTableSerDe) {
    if (!_registered) {
      this._dataTableSerDe = dataTableSerDe;
      _registered = true;
    } else {
      LOGGER.warn("DataTable serializer/de-serializer {} already registered, ignoring {}",
          _dataTableSerDe.getClass().getName(), dataTableSerDe.getClass().getName());
    }
  }

  /**
   * Method to get the registered DataTableSerDe.
   * <p> - Returns the DataTableSerDe, if one was registered.</p>
   * <p> - Returns the default {@link DataTableJavaSerDe}, if none was registered. </p>
   *
   * @return Registered (or default) DataTableSerDe
   */
  public DataTableSerDe get() {
    return (_dataTableSerDe != null) ? _dataTableSerDe : _defaultDataTableSerDe;
  }
}
