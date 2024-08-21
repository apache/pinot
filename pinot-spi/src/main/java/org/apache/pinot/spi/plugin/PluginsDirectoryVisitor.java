package org.apache.pinot.spi.plugin;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.HashSet;


class PluginsDirectoryVisitor extends SimpleFileVisitor<Path> {

  private final Collection<Path> _pluginDirs = new HashSet<>();

  private final PinotPluginHandler _pluginHandler = new PinotPluginHandler();

  @Override
  public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
      throws IOException {
    if("classes".endsWith(file.getFileName().toString()) ||
        "lib".endsWith(file.getFileName().toString()) ||
        file.getFileName().toString().endsWith(".jar")) {
      _pluginDirs.add(file.getParent());
      return FileVisitResult.SKIP_SIBLINGS;
    }

    if(file.getFileName().toString().endsWith(".zip")) {
      _pluginHandler.unpack(file.getParent().toFile(), file.toFile());
    }

    return super.visitFile(file, attrs);
  }

  public Collection<Path> getPluginDirs() {
    return _pluginDirs;
  }
}
