package org.apache.pinot.controller.helix.core.realtime;

// SegmentCompletionFSMFactory.java
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;

public class SegmentCompletionFSMFactory {
  private SegmentCompletionFSMFactory() {
    // Private constructor to prevent instantiation
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentCompletionFSMFactory.class);
  private static final String CLASS = "class";
  private static final Map<String, SegmentCompletionFSMCreator> FSM_CREATOR_MAP = new HashMap<>();

  /**
   * Functional interface for creating FSM instances.
   */
  @FunctionalInterface
  public interface SegmentCompletionFSMCreator {
    SegmentCompletionFSM create(SegmentCompletionManager manager,
        PinotLLCRealtimeSegmentManager segmentManager,
        LLCSegmentName llcSegmentName,
        SegmentZKMetadata segmentMetadata,
        String msgType);
  }

  /**
   * Registers an FSM creator with a specific scheme/type.
   *
   * @param scheme The scheme or type key.
   * @param creator The creator instance.
   */
  public static void register(String scheme, SegmentCompletionFSMCreator creator) {
    Preconditions.checkNotNull(scheme, "Scheme cannot be null");
    Preconditions.checkNotNull(creator, "FSM Creator cannot be null");
    if (FSM_CREATOR_MAP.containsKey(scheme)) {
      LOGGER.warn("Overwriting existing FSM creator for scheme {}", scheme);
    }
    FSM_CREATOR_MAP.put(scheme, creator);
    LOGGER.info("Registered SegmentCompletionFSM creator for scheme {}", scheme);
  }

  /**
   * Initializes the factory with configurations.
   *
   * @param fsmFactoryConfig The configuration object containing FSM schemes and classes.
   */
  public static void init(SegmentCompletionConfig fsmFactoryConfig) {
    // Assuming SegmentCompletionConfig is a wrapper around configuration properties
    Map<String, String> schemesConfig = fsmFactoryConfig.getFsmSchemes();
    for (Map.Entry<String, String> entry : schemesConfig.entrySet()) {
      String scheme = entry.getKey();
      String className = entry.getValue();
      try {
        LOGGER.info("Initializing SegmentCompletionFSM for scheme {}, classname {}", scheme, className);
        Class<?> clazz = Class.forName(className);
        SegmentCompletionFSMCreator creator = (SegmentCompletionFSMCreator) clazz.getDeclaredConstructor().newInstance();
        register(scheme, creator);
      } catch (Exception e) {
        LOGGER.error("Could not instantiate FSM for class {} with scheme {}", className, scheme, e);
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
   * @param msgType The message type.
   * @return An instance of SegmentCompletionFSMInterface.
   */
  public static SegmentCompletionFSM createFSM(String scheme,
      SegmentCompletionManager manager,
      PinotLLCRealtimeSegmentManager segmentManager,
      LLCSegmentName llcSegmentName,
      SegmentZKMetadata segmentMetadata,
      String msgType) {
    SegmentCompletionFSMCreator creator = FSM_CREATOR_MAP.get(scheme);
    Preconditions.checkState(creator != null, "No FSM registered for scheme: " + scheme);
    return creator.create(manager, segmentManager, llcSegmentName, segmentMetadata, msgType);
  }

  /**
   * Checks if a scheme is supported.
   *
   * @param factoryType The scheme to check.
   * @return True if supported, false otherwise.
   */
  public static boolean isFactoryTypeSupported(String factoryType) {
    return FSM_CREATOR_MAP.containsKey(factoryType);
  }

  /**
   * Shuts down all registered FSMs if needed.
   * (Implement if FSMs require shutdown logic)
   */
  public static void shutdown() {
    // Implement shutdown logic if FSMs have resources to release
  }
}

