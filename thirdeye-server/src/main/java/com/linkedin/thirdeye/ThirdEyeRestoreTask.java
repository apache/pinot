package com.linkedin.thirdeye;

import com.google.common.collect.ImmutableMultimap;
import com.linkedin.thirdeye.api.StarTreeManager;
import io.dropwizard.servlets.tasks.Task;

import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URI;

public class ThirdEyeRestoreTask extends Task
{
  private final StarTreeManager manager;

  public ThirdEyeRestoreTask(StarTreeManager manager)
  {
    super("restore");
    this.manager = manager;
  }

  @Override
  public void execute(ImmutableMultimap<String, String> params, PrintWriter printWriter) throws Exception
  {
    String collection = params.get("collection").iterator().next();
    URI treeUri = URI.create(params.get("treeUri").iterator().next());
    URI configUri = URI.create(params.get("configUri").iterator().next());

    printWriter.append("Loading tree from ").append(treeUri.toASCIIString());
    InputStream treeStream = treeUri.toURL().openStream();

    printWriter.append("Loading config from ").append(configUri.toASCIIString());
    InputStream configStream = configUri.toURL().openStream();

    printWriter.append("Restoring tree for ").append(collection);
    manager.restore(collection, treeStream, configStream);

    printWriter.append("Done!");
  }
}
