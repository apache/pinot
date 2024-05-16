package org.apache.pinot.common.upsert.hash;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.spi.utils.PinotReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UpsertHashFunctionFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(UpsertHashFunctionFactory.class);
  private static final Map<String, Class<UpsertHashFunction>> HASH_FUNCTION_MAP = new HashMap<>();
  private static final Object INIT_LOCK = new Object();
  private volatile static boolean _isLoaded = false;

  private UpsertHashFunctionFactory() {
  }

  public static UpsertHashFunction create(String functionName) {
    if (!_isLoaded) {
      synchronized (INIT_LOCK) {
        if (!_isLoaded) {
          try {
            load();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          _isLoaded = true;
        }
      }
    }
    try {
      functionName = functionName.toUpperCase();
      if (!HASH_FUNCTION_MAP.containsKey(functionName)) {
        throw new IllegalArgumentException("Unsupported hash function: " + functionName);
      }
      return createInstance(HASH_FUNCTION_MAP.get(functionName));
    } catch (Exception e) {
      throw new RuntimeException("Caught exception creating upsert hash function: " + functionName, e);
    }
  }

  private static void load()
      throws Exception {
    long startTime = System.currentTimeMillis();
    try {
      Set<Class<?>> implementations = PinotReflectionUtils.getImplementationsOfInterface(
          UpsertHashFunction.class, ".*function.hash.*");
      for (Class<?> implementation : implementations) {
        if (UpsertHashFunction.class.isAssignableFrom(implementation)) {
          UpsertHashFunction upsertHashFunction = createInstance((Class<UpsertHashFunction>) implementation);
          HASH_FUNCTION_MAP.put(upsertHashFunction.getId().toUpperCase(),
              (Class<UpsertHashFunction>) implementation);
        }
      }
    } finally {
      LOGGER.info("Loaded {} upsert hash functions in {}ms", HASH_FUNCTION_MAP.size(),
          System.currentTimeMillis() - startTime);
    }
  }

  private static UpsertHashFunction createInstance(Class<UpsertHashFunction> klass)
      throws Exception {
    Constructor<UpsertHashFunction> constructor = klass.getDeclaredConstructor();
    return constructor.newInstance();
  }
}
