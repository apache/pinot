package com.linkedin.pinot.segments.v1.creator;

import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


/**
* @author Dhaval Patel<dpatel@linkedin.com>
* Aug 15, 2014
*/

public class ParallelSegmentCreator {

  private final static ExecutorService executorService = Executors.newFixedThreadPool(10);
  private static File baseDir;
  private static File baseAvro;

  public static void main(String[] args) throws InterruptedException, ExecutionException {
    baseDir = new File("/home/dpatel/experiments/data/segments");
    baseAvro = new File("/home/dpatel/experiments/data/fromHadoop");

    int counter = 0;
    Future<String>[] futures = new Future[baseAvro.listFiles().length];
    for (File f : baseAvro.listFiles()) {
      futures[counter] = executorService.submit(new SegmentCreatorCallable(f, baseDir, "segment" + counter));
      counter++;
    }

    for (Future f : futures) {
      System.out.println(f.get());
    }

  }
}
