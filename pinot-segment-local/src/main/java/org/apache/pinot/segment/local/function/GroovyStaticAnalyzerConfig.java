package org.apache.pinot.segment.local.function;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;


public class GroovyStaticAnalyzerConfig {
  final boolean _enabled;
  final private List<String> _allowedReceivers;
  final private List<String> _allowedImports;
  final private List<String> _allowedStaticImports;

  final private List<String> _disallowedMethodNames;

  public GroovyStaticAnalyzerConfig(
      @JsonProperty("enabled")
      boolean enabled,
      @JsonProperty("allowedReceivers")
      List<String> allowedReceivers,
      @JsonProperty("allowedImports")
      List<String> allowedImports,
      @JsonProperty("allowedStaticImports")
      List<String> allowedStaticImports,
      @JsonProperty("disallowedMethodNames")
      List<String> disallowedMethodNames) {
    _enabled = enabled;
    _allowedImports = allowedImports;
    _allowedReceivers = allowedReceivers;
    _allowedStaticImports = allowedStaticImports;
    _disallowedMethodNames = disallowedMethodNames;
  }

  @JsonProperty("enabled")
  public boolean isEnabled() {
    return _enabled;
  }

  @JsonProperty("allowedReceivers")
  public List<String> getAllowedReceivers() {
    return _allowedReceivers;
  }

  @JsonProperty("allowedImports")
  public List<String> getAllowedImports() {
    return _allowedImports;
  }

  @JsonProperty("allowedStaticImports")
  public List<String> getAllowedStaticImports() {
    return _allowedStaticImports;
  }

  @JsonProperty("disallowedMethodNames")
  public List<String> getDisallowedMethodNames() {
    return _disallowedMethodNames;
  }

  public static List<Class> getDefaultAllowedTypes() {
    return List.of(
        Integer.class,
        Float.class,
        Long.class,
        Double.class,
        Integer.TYPE,
        Long.TYPE,
        Float.TYPE,
        Double.TYPE,
        String.class,
        Object.class,
        Byte.class,
        Byte.TYPE,
        BigDecimal.class,
        BigInteger.class
    );
  }

  public static List<String> getDefaultAllowedReceivers() {
    return List.of(String.class.getName(), Math.class.getName());
  }

  public static List<String> getDefaultAllowedImports() {
    return List.of(Math.class.getName());
  }
}
