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
package com.linkedin.pinot.core.data.readers;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class ThriftRecordReader implements RecordReader {

    private final Schema _schema;

    private final ThriftRecordReaderConfig _recordReaderConfig;

    private final Class<TBase<?, ?>> _thriftClass;

    /**
     * For reading the binary thrift objects.
     */
    private TBinaryProtocol _binaryIn;

    private BufferedInputStream _bufferIn;

    private final File _dataFile;

    Map<String, Integer> _fieldNameToIndexMap;

    public ThriftRecordReader(File dataFile, Schema schema, ThriftRecordReaderConfig recordReaderConfig) throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        this._schema = schema;
        this._dataFile = dataFile;
        this._recordReaderConfig = recordReaderConfig;
        this._thriftClass = initThriftInstanceCreator();
        this._bufferIn = RecordReaderUtils.getFileBufferStream(dataFile);
        this._binaryIn = new TBinaryProtocol(new TIOStreamTransport(_bufferIn));
        this._fieldNameToIndexMap = new HashMap();
        TBase t = this._thriftClass.newInstance();
        int index = 1;
        TFieldIdEnum fieldIdEnum = null;
        do {
            fieldIdEnum = t.fieldForId(index);
            if (fieldIdEnum != null) {
                _fieldNameToIndexMap.put(fieldIdEnum.getFieldName(), index);
            }
            index = index + 1;
        } while (fieldIdEnum != null);
    }

    @Override
    public boolean hasNext() {
        _bufferIn.mark(1);
        int val = 0;
        try {
            val = _bufferIn.read();
            _bufferIn.reset();
        } catch (IOException e) {
            val = -1;
            throw new RuntimeException("Error in iterating Reader", e);
        }
        return val != -1;
    }

    @Override
    public GenericRow next() throws IOException {
        return next(new GenericRow());
    }

    @Override
    public GenericRow next(GenericRow reuse) throws IOException {
        TBase t = null;
        try {
            t = this._thriftClass.newInstance();
            t.read(_binaryIn);
        } catch (Exception e) {
            throw new RuntimeException("Caught exception while serialize thrift instance", e);
        }
        for (FieldSpec fieldSpec : _schema.getAllFieldSpecs()) {
            String fieldName = fieldSpec.getName();
            if (_fieldNameToIndexMap.containsKey(fieldName)) {
                int tFieldId = _fieldNameToIndexMap.get(fieldName);
                TFieldIdEnum tFieldIdEnum = t.fieldForId(tFieldId);
                Object thriftValue = t.getFieldValue(tFieldIdEnum);
                Object value = null;
                if (fieldSpec.isSingleValueField()) {
                    String token = thriftValue != null ? thriftValue.toString() : null;
                    value = RecordReaderUtils.convertToDataType(token, fieldSpec);
                } else {
                    if(thriftValue instanceof ArrayList) {
                        value = RecordReaderUtils.convertToDataTypeArray((ArrayList) thriftValue, fieldSpec);
                    } else if(thriftValue instanceof HashSet) {
                        value = RecordReaderUtils.convertToDataTypeSet((HashSet) thriftValue, fieldSpec);
                    }
                }
                reuse.putField(fieldName, value);
            }
        }
        return reuse;
    }

    @Override
    public void rewind() throws IOException {
        _bufferIn = RecordReaderUtils.getFileBufferStream(_dataFile);
        _binaryIn = new TBinaryProtocol(new TIOStreamTransport(_bufferIn));
    }

    @Override
    public Schema getSchema() {
        return _schema;
    }

    @Override
    public void close() throws IOException {
        _bufferIn.close();
    }

    private Class<TBase<?, ?>> initThriftInstanceCreator() {
        Class<TBase<?, ?>> tBase = null;
        if (_recordReaderConfig == null || _recordReaderConfig.getThriftClass() == null) {
            throw new IllegalArgumentException("Thrift class not found in the configuration");
        }
        try {
            tBase = (Class<TBase<?, ?>>) Class.forName(_recordReaderConfig.getThriftClass());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Caught exception while thrift class initialize", e);
        }
        return tBase;
    }
}
