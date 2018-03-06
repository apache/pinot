package com.linkedin.pinot.tools.pacelab.benchmark;

import java.io.IOException;
import java.nio.file.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

public class FileWatchService implements Runnable{

    public static final String defaultPath = "pinot_benchmark/query_generator_config/";
    private Map<String,QueryExecutor> executorMap = null;

    public void setExecutorList(List<QueryExecutor> executorList){
        executorMap = new HashMap<>();
        for(QueryExecutor executor:executorList){
            executorMap.put(executor.getConfigFile(),executor);
        }
    }

    @Override
    public void run() {
        try {
            WatchService watcher = FileSystems.getDefault().newWatchService();
            String path = System.getenv("PINOT_HOME");
            if(path==null){
                //TODO We can load config from class loader also as default config to handle null pointer exception
                System.out.println("Environment variable is null");
                return;
            }
            if(path.endsWith("/"))
            {
                path = path + defaultPath;
            }
            else
            {
                path = path + "/" + defaultPath;
            }
            Path dir = Paths.get(path);
            dir.register(watcher, ENTRY_MODIFY);

            System.out.println("Watch Service registered for dir: " + dir.getFileName());

            while (true) {
                WatchKey key;
                try {
                    key = watcher.take();
                } catch (InterruptedException ex) {
                    return;
                }

                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    @SuppressWarnings("unchecked")
                    WatchEvent<Path> ev = (WatchEvent<Path>) event;
                    Path fileName = ev.context();

                    System.out.println(kind.name() + ": " + fileName);

                    if (kind == ENTRY_MODIFY)//&&
                            //fileName.toString().equals("DirectoryWatchDemo.java"))
                    {
                        if(executorMap.get(fileName.toString())!=null){
                            executorMap.get(fileName.toString()).loadConfig();
                        }
                        System.out.println("My source file has changed!!!"+fileName.toString());
                    }
                }

                boolean valid = key.reset();
                if (!valid) {
                    break;
                }
            }

        } catch (IOException ex) {
            System.err.println(ex);
        }
    }
}
