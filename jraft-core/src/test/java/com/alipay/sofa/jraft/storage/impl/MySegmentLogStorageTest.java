package com.alipay.sofa.jraft.storage.impl;

import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.storage.log.MyRocksDBSegmentLogStorage;

/**
 * @author: caobiao
 * @date: 2020/07/22
 * @Description:
 */
public class MySegmentLogStorageTest extends BaseLogStorageTest {

    @Override
    protected LogStorage newLogStorage() {
        return new MyRocksDBSegmentLogStorage(this.path , new RaftOptions());
    }

}
