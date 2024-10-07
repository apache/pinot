package org.apache.pinot.core.data.function;

import java.util.List;
import org.apache.pinot.segment.local.function.GroovyStaticAnalyzerConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test serialization and deserialization.
 */
public class GroovyStaticAnalyzerConfigTest {
  @Test
  public void testEmptyConfig() throws Exception {
    GroovyStaticAnalyzerConfig config = new GroovyStaticAnalyzerConfig(false, null, null, null, null);
    String encodedConfig = JsonUtils.objectToString(config);
    GroovyStaticAnalyzerConfig decodedConfig = JsonUtils.stringToObject(encodedConfig, GroovyStaticAnalyzerConfig.class);

    Assert.assertFalse(decodedConfig.isEnabled());
    Assert.assertNull(decodedConfig.getAllowedReceivers());
    Assert.assertNull(decodedConfig.getAllowedImports());
    Assert.assertNull(decodedConfig.getAllowedStaticImports());
    Assert.assertNull(decodedConfig.getDisallowedMethodNames());
  }

  @Test
  public void testAllowedReceivers() throws Exception {
    GroovyStaticAnalyzerConfig config = new GroovyStaticAnalyzerConfig(false,
        GroovyStaticAnalyzerConfig.getDefaultAllowedReceivers(),
        null,
        null,
        null);
    String encodedConfig = JsonUtils.objectToString(config);
    GroovyStaticAnalyzerConfig decodedConfig = JsonUtils.stringToObject(encodedConfig, GroovyStaticAnalyzerConfig.class);

    Assert.assertFalse(decodedConfig.isEnabled());
    Assert.assertEquals(GroovyStaticAnalyzerConfig.getDefaultAllowedReceivers(), decodedConfig.getAllowedReceivers());
    Assert.assertNull(decodedConfig.getAllowedImports());
    Assert.assertNull(decodedConfig.getAllowedStaticImports());
    Assert.assertNull(decodedConfig.getDisallowedMethodNames());
  }

  @Test
  public void testAllowedImports() throws Exception {
    GroovyStaticAnalyzerConfig config = new GroovyStaticAnalyzerConfig(false,
        null,
        GroovyStaticAnalyzerConfig.getDefaultAllowedImports(),
        null,
        null);
    String encodedConfig = JsonUtils.objectToString(config);
    GroovyStaticAnalyzerConfig decodedConfig = JsonUtils.stringToObject(encodedConfig, GroovyStaticAnalyzerConfig.class);

    Assert.assertFalse(decodedConfig.isEnabled());
    Assert.assertNull(decodedConfig.getAllowedReceivers());
    Assert.assertEquals(GroovyStaticAnalyzerConfig.getDefaultAllowedImports(), decodedConfig.getAllowedImports());
    Assert.assertNull(decodedConfig.getAllowedStaticImports());
    Assert.assertNull(decodedConfig.getDisallowedMethodNames());
  }

  @Test
  public void testAllowedStaticImports() throws Exception {
    GroovyStaticAnalyzerConfig config = new GroovyStaticAnalyzerConfig(false,
        null,
        null,
        GroovyStaticAnalyzerConfig.getDefaultAllowedImports(),
        null);
    String encodedConfig = JsonUtils.objectToString(config);
    GroovyStaticAnalyzerConfig decodedConfig = JsonUtils.stringToObject(encodedConfig, GroovyStaticAnalyzerConfig.class);

    Assert.assertFalse(decodedConfig.isEnabled());
    Assert.assertNull(decodedConfig.getAllowedReceivers());
    Assert.assertNull(decodedConfig.getAllowedImports());
    Assert.assertEquals(GroovyStaticAnalyzerConfig.getDefaultAllowedImports(), decodedConfig.getAllowedStaticImports());
    Assert.assertNull(decodedConfig.getDisallowedMethodNames());
  }

  @Test
  public void testDisallowedMethodNames() throws Exception {
    GroovyStaticAnalyzerConfig config = new GroovyStaticAnalyzerConfig(false,
        null,
        null,
        null,
        List.of("method1", "method2"));
    String encodedConfig = JsonUtils.objectToString(config);
    GroovyStaticAnalyzerConfig decodedConfig = JsonUtils.stringToObject(encodedConfig, GroovyStaticAnalyzerConfig.class);

    Assert.assertFalse(decodedConfig.isEnabled());
    Assert.assertNull(decodedConfig.getAllowedReceivers());
    Assert.assertNull(decodedConfig.getAllowedImports());
    Assert.assertNull(decodedConfig.getAllowedStaticImports());
    Assert.assertEquals(List.of("method1", "method2"), decodedConfig.getDisallowedMethodNames());
  }
}
