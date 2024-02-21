/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.integration.tests;

import io.netty.util.ResourceLeakDetector.LeakListener;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class NettyLeakListener implements LeakListener {
    private final List<String> _leaks = new CopyOnWriteArrayList<>();

    @Override
    public void onLeak(String resourceType, String records) {
        _leaks.add(resourceType);
    }

    public int getLeakCount() {
        return _leaks.size();
    }

    public void assertZeroLeaks() {
        assertZeroLeaks(null);
    }

    public void assertZeroLeaks(String detail) {
        if (!_leaks.isEmpty()) {
            StringBuilder message = new StringBuilder("Netty leaks: ");
            if (detail != null) {
                message.append(detail);
                message.append(" ");
            }
            message.append(_leaks);
            throw new IllegalStateException(message.toString());
        }
    }

    @Override
    public String toString() {
        return "leakCount=" + this.getLeakCount();
    }
}
