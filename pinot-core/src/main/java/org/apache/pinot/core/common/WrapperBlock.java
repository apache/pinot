package org.apache.pinot.core.common;

import org.apache.pinot.core.operator.blocks.TransformBlock;

public interface WrapperBlock<T> extends Block {
    TransformBlock getTransformBlock(T key);
}
