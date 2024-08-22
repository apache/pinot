package org.apache.pinot.spi.plugin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


public class PluginsDirectoryVisitorTest {

  private final PinotPluginHandler _pinotPluginHandler = new PinotPluginHandler();
  private final ShadedPluginHandler _shadedPluginHandler = new ShadedPluginHandler();

  @Test
  public void testVisitDirectory() {
    PluginsDirectoryVisitor visitor = new PluginsDirectoryVisitor(_pinotPluginHandler, _shadedPluginHandler);

    Path pluginsDirectory = Path.of("src/test/resources/plugins-directory");
    try {
      Files.walkFileTree(pluginsDirectory, Set.of(), 1, visitor);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    assertEquals(visitor.getPluginDirs().size(), 3);
    assertTrue(visitor.getPluginDirs().contains(Path.of("src/test/resources/plugins-directory/shadedplugin-1")));
    assertTrue(visitor.getPluginDirs().contains(Path.of("src/test/resources/plugins-directory/pinotplugin-1")));
    assertTrue(visitor.getPluginDirs().contains(Path.of("src/test/resources/plugins-directory/pinotplugin-2")));
  }
}