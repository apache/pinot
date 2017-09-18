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

package com.linkedin.pinot.core.realtime.impl.kafka;

/**
 * Interface wrapping Kafka consumer. Throws IndexOutOfBoundsException when trying to access a message at an
 * invalid index.
 * @param <T>
 */
public interface MessageBatch<T> {
    /**
     *
     * @return number of messages returned from kafka
     */
    int getMessageCount();

    /**
     * Returns the message at a particular index inside a set of messages returned from Kafka.
     * @param index
     * @return
     */
    T getMessageAtIndex(int index);

    /**
     * Returns the offset of the message at a particular index inside a set of messages returned from Kafka.
     * @param index
     * @return
     */
    int getMessageOffsetAtIndex(int index);

    /**
     * Returns the length of the message at a particular index inside a set of messages returned from Kafka.
     * @param index
     * @return
     */
    int getMessageLengthAtIndex(int index);

    /**
     * Returns the offset of the next message.
     * @param index
     * @return
     */
    long getNextKafkaMessageOffsetAtIndex(int index);
}