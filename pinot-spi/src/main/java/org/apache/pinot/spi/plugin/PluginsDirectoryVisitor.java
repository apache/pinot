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

  private final PinotPluginHandler _pinotPluginHandler;

  private final ShadedPluginHandler _shadedPluginHandler;

  public PluginsDirectoryVisitor(PinotPluginHandler _pinotPluginHandler, ShadedPluginHandler _shadedPluginHandler) {
    this._pinotPluginHandler = _pinotPluginHandler;
    this._shadedPluginHandler = _shadedPluginHandler;
  }

  @Override
  public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
      throws IOException {
    if(_pinotPluginHandler.isPluginDirectory(file) || _shadedPluginHandler.isPluginDirectory(file)) {
      _pluginDirs.add(file);
      return FileVisitResult.CONTINUE;
    }

    return super.visitFile(file, attrs);
  }

  @Override
  public FileVisitResult visitFileFailed(Path file, IOException exc)
      throws IOException {
    return super.visitFileFailed(file, exc);
  }

  public Collection<Path> getPluginDirs() {
    return _pluginDirs;
  }
}
