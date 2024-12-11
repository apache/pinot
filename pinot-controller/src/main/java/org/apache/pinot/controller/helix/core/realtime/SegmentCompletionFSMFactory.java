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
package org.apache.pinot.controller.helix.core.realtime;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentCompletionFSMFactory {
  private SegmentCompletionFSMFactory() {
    // Private constructor to prevent instantiation
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentCompletionFSMFactory.class);
  private static final Map<String, Class<? extends SegmentCompletionFSM>> FSM_CLASS_MAP = new HashMap<>();
  public static final String DEFAULT = "default";

  // Static block to register the default FSM
  static {
    try {
      Class<?> clazz = Class.forName(BlockingSegmentCompletionFSM.class.getCanonicalName());
      register(DEFAULT, (Class<? extends SegmentCompletionFSM>) clazz);
      LOGGER.info("Registered default BlockingSegmentCompletionFSM");
    } catch (Exception e) {
      LOGGER.error("Failed to register default BlockingSegmentCompletionFSM", e);
      throw new RuntimeException("Failed to register default BlockingSegmentCompletionFSM", e);
    }
  }

  /**
   * Registers an FSM class with a specific scheme/type.
   *
   * @param scheme The scheme or type key.
   * @param fsmClass The class for the FSM.
   */
  public static void register(String scheme, Class<? extends SegmentCompletionFSM> fsmClass) {
    Preconditions.checkNotNull(scheme, "Scheme cannot be null");
    Preconditions.checkNotNull(fsmClass, "FSM Class cannot be null");
    if (FSM_CLASS_MAP.containsKey(scheme)) {
      LOGGER.warn("Overwriting existing FSM class for scheme {}", scheme);
    }
    FSM_CLASS_MAP.put(scheme, fsmClass);
    LOGGER.info("Registered SegmentCompletionFSM class for scheme {}", scheme);
  }

  /**
   * Initializes the factory with configurations.
   *
   * @param fsmFactoryConfig The configuration object containing FSM schemes and classes.
   */
  public static void init(SegmentCompletionConfig fsmFactoryConfig) {
    Map<String, String> schemesConfig = fsmFactoryConfig.getFsmSchemes();
    for (Map.Entry<String, String> entry : schemesConfig.entrySet()) {
      String scheme = entry.getKey();
      String className = entry.getValue();
      try {
        LOGGER.info("Initializing SegmentCompletionFSM for scheme {}, classname {}", scheme, className);
        Class<?> clazz = Class.forName(className);
        register(scheme, (Class<? extends SegmentCompletionFSM>) clazz);
      } catch (Exception e) {
        LOGGER.error("Could not register FSM class for class {} with scheme {}", className, scheme, e);
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Creates an FSM instance based on the scheme/type.
   *
   * @param scheme The scheme or type key.
   * @param manager The SegmentCompletionManager instance.
   * @param segmentManager The PinotLLCRealtimeSegmentManager instance.
   * @param llcSegmentName The segment name.
   * @param segmentMetadata The segment metadata.
   * @return An instance of SegmentCompletionFSM.
   */
  public static SegmentCompletionFSM createFSM(String scheme,
      SegmentCompletionManager manager,
      PinotLLCRealtimeSegmentManager segmentManager,
      LLCSegmentName llcSegmentName,
      SegmentZKMetadata segmentMetadata) {
    Class<? extends SegmentCompletionFSM> fsmClass = FSM_CLASS_MAP.get(scheme);
    Preconditions.checkState(fsmClass != null, "No FSM registered for scheme: " + scheme);
    try {
      return fsmClass.getConstructor(
          PinotLLCRealtimeSegmentManager.class,
          SegmentCompletionManager.class,
          LLCSegmentName.class,
          SegmentZKMetadata.class
      ).newInstance(segmentManager, manager, llcSegmentName, segmentMetadata);
    } catch (Exception e) {
      LOGGER.error("Failed to create FSM instance for scheme {}", scheme, e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Checks if a scheme is supported.
   *
   * @param factoryType The scheme to check.
   * @return True if supported, false otherwise.
   */
  public static boolean isFactoryTypeSupported(String factoryType) {
    return FSM_CLASS_MAP.containsKey(factoryType);
  }

  /**
   * Clears all registered FSM classes.
   */
  public static void shutdown() {
    FSM_CLASS_MAP.clear();
  }
}
