/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.tools.pacelab.benchmark;

import java.io.IOException;
import java.nio.file.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

public class FileWatchService implements Runnable{

    private static final String _defaultPath = "pinot_benchmark/query_generator_config/";
    private Map<String,QueryExecutor> _executorMap = null;

    public void setExecutorList(List<QueryExecutor> executorList){
        _executorMap = new HashMap<>();
        for(QueryExecutor executor:executorList){
            _executorMap.put(executor.getConfigFile(),executor);
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
                path = path + _defaultPath;
            }
            else
            {
                path = path + "/" + _defaultPath;
            }
            Path dir = Paths.get(path);
            dir.register(watcher, ENTRY_MODIFY);

            System.out.println("Watch Service registered for dir: " + dir.getFileName());

            while (true) {
                WatchKey key;
                try {
                    key = watcher.take();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
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
                        if(_executorMap.get(fileName.toString())!=null){
                            _executorMap.get(fileName.toString()).loadConfig();
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
