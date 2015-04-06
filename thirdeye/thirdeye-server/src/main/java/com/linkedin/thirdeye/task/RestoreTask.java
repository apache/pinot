package com.linkedin.thirdeye.task;

import com.google.common.collect.ImmutableMultimap;
import com.linkedin.thirdeye.api.StarTreeManager;
import io.dropwizard.servlets.tasks.Task;

import java.io.File;
import java.io.PrintWriter;
import java.util.Collection;

public class RestoreTask extends Task
{
  private final StarTreeManager manager;
  private final File rootDir;

  public RestoreTask(StarTreeManager manager, File rootDir)
  {
    super("restore");
    this.manager = manager;
    this.rootDir = rootDir;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> params, PrintWriter printWriter) throws Exception
  {
    Collection<String> collectionParam = params.get("collection");
    if (collectionParam == null || collectionParam.isEmpty())
    {
      throw new IllegalArgumentException("Must provide collection");
    }
    String collection = collectionParam.iterator().next();
    manager.restore(rootDir, collection);
  }
}
