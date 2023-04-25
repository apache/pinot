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
package org.apache.pinot.core.segment.processing.lifecycle;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixManager;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotSegmentLifecycleEventListenerManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentLifecycleEventListenerManager.class);
  private static final PinotSegmentLifecycleEventListenerManager INSTANCE =
      new PinotSegmentLifecycleEventListenerManager();
  private Map<SegmentLifecycleEventType, List<PinotSegmentLifecycleEventListener>> _eventTypeToListenersMap;
  private boolean _initialized = false;

  private PinotSegmentLifecycleEventListenerManager() {
  }

  public static PinotSegmentLifecycleEventListenerManager getInstance() {
    return INSTANCE;
  }

  public synchronized void init(HelixManager helixZkManager) {
    if (_initialized) {
      LOGGER.warn("Segment lifecycle event listener manager already initialized, skipping it");
      return;
    }
    _eventTypeToListenersMap = new HashMap<>();
    Set<Class<?>> classes =
        PinotReflectionUtils.getClassesThroughReflection(".*\\.plugin\\.segment\\.lifecycle\\.listener\\..*",
            SegmentLifecycleEventListener.class);
    for (Class<?> clazz : classes) {
      SegmentLifecycleEventListener annotation = clazz.getAnnotation(SegmentLifecycleEventListener.class);
      if (annotation.enabled()) {
        try {
          PinotSegmentLifecycleEventListener pinotSegmentLifecycleEventListener =
              (PinotSegmentLifecycleEventListener) clazz.newInstance();
          pinotSegmentLifecycleEventListener.init(helixZkManager);
          _eventTypeToListenersMap.compute(pinotSegmentLifecycleEventListener.getType(), (key, list) -> {
            if (list == null) {
              list = new ArrayList<>();
            }
            list.add(pinotSegmentLifecycleEventListener);
            return list;
          });
        } catch (Exception e) {
          LOGGER.error("Caught exception while initializing segment lifecyle event listener : {}, skipping it", clazz,
              e);
        }
      }
    }

    _initialized = true;
  }

  public void notifyListeners(SegmentLifecycleEventDetails event) {
    if (!_initialized) {
      LOGGER.warn("Segment lifecycle event listener manager not initialized, skipping it");
      return;
    }

    List<PinotSegmentLifecycleEventListener> listeners = _eventTypeToListenersMap.get(event.getType());
    if (listeners != null) {
      for (PinotSegmentLifecycleEventListener listener : listeners) {
        try {
          listener.onEvent(event);
        } catch (Exception e) {
          LOGGER.error("Segment lifecycle listener call failed : ", e);
        }
      }
    }
  }
}
