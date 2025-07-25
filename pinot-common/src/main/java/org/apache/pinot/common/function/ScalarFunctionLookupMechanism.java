package org.apache.pinot.common.function;

import java.util.Set;


/**
 * Interface for looking up scalar functions.
 *
 * Each instance of this interface represents a mechanism to look up scalar functions. They should be registered
 * as a service provider in the META-INF/services directory to be discovered by the ServiceLoader.
 * Alternatively, they can be registered using the {@link com.google.auto.service.AutoService} annotation.
 *
 * These services will rarely be used, usually only once at startup.
 *
 * @see AnnotatedClassLookupMechanism
 * @see AnnotatedMethodLookupMechanism
 */
public interface ScalarFunctionLookupMechanism {

  /**
   * Returns the set of {@link ScalarFunctionProvider} instances that can be used to look up scalar functions.
   */
  Set<ScalarFunctionProvider> getProviders();

  /**
   * Interface for providing scalar functions.
   * <p>Each provider can provide multiple scalar functions, all with the same priority.
   * <p>If two functions have the same canonical name (which means they are found by different providers),
   * the one with higher priority will be used.
   */
  interface ScalarFunctionProvider {
    /**
     * Returns the name of the provider, not the functions it provides.
     *
     * <p>This is used for logging and debugging purposes when there are multiple providers for the same function.
     */
    String name();

    /**
     * Returns the priority of the provider. In case two functions have the same canonical name,
     * the one with higher priority will be used.
     *
     * <p>Default priority is 0.
     */
    default int priority() {
      return 0;
    }

    /**
     * Returns a set of {@link PinotScalarFunction} instances.
     */
    Set<PinotScalarFunction> getFunctions();
  }
}
