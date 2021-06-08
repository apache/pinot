package org.apache.pinot.plugin.stream.pulsar;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.stream.Collectors.joining;


public class DockerExecutor {

  public String execute(String command, long waitTimeoutMillis) {
    try {

      String[] commandList = command.split(" ");

      Process process = new ProcessBuilder().command(commandList).redirectErrorStream(true).start();

      ExecutorService exec = newSingleThreadExecutor();
      Future<String> outputFuture = exec.submit(() -> handleOutput(process));

      String output = outputFuture.get(waitTimeoutMillis, TimeUnit.MILLISECONDS);
      process.waitFor(waitTimeoutMillis, TimeUnit.MILLISECONDS);

      int code = process.exitValue();
      exec.shutdown();

      if (code != 0) {
        throw new RuntimeException("Error status code " + code + " returned from docker. Output: " + output);
      }

      return output;
    } catch (Exception ex) {
      throw new RuntimeException("Failed to execute command", ex);
    }
  }

  private String handleOutput(Process process) {
    BufferedReader stdout = new BufferedReader(new InputStreamReader(process.getInputStream(), UTF_8));
    return stdout.lines().collect(joining(System.lineSeparator()));
  }
}
