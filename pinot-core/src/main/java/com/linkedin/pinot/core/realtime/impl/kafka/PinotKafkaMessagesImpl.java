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

import com.linkedin.pinot.core.data.GenericRow;
import java.util.Iterator;
import kafka.message.MessageAndOffset;


public class PinotKafkaMessagesImpl implements PinotKafkaMessagesIterable {

  private Iterable<MessageAndOffset> _messageAndOffsetIterable;

  public PinotKafkaMessagesImpl(Iterable<MessageAndOffset> messageAndOffsetIterable) {
    _messageAndOffsetIterable = messageAndOffsetIterable;
  }

  public PinotKafkaMessagesImpl getMessages() {
   return this;
  }

  public PinotKafkaMessageAndOffset decodeMessageAndOffset(GenericRow decodedRow, Object message, Object decoder) {
    MessageAndOffset messageAndOffset = (MessageAndOffset) message;
    byte[] array = messageAndOffset.message().payload().array();
    int offset = messageAndOffset.message().payload().arrayOffset();
    int length = messageAndOffset.message().payloadSize();
    decodedRow = GenericRow.createOrReuseRow(decodedRow);

    KafkaMessageDecoder messageDecoder = (KafkaMessageDecoder) decoder;
    return new PinotKafkaMessageAndOffset(messageDecoder.decode(array, offset, length, decodedRow), messageAndOffset.offset(), messageAndOffset.nextOffset());
  }

  public Iterator iterator() {
    return _messageAndOffsetIterable.iterator();
  }
}
