package com.alipay.sofa.jraft.storage.impl;

import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.storage.log.ArrayDequeSegmentLogStorage;

/**
 * @author: caobiao
 * @date: 2020/07/22
 * @Description:
 */
public class ArrayDequeLogStorageTest extends BaseLogStorageTest {

    @Override
    protected LogStorage newLogStorage() {
        return new ArrayDequeSegmentLogStorage(this.path , new RaftOptions());
    }

}
