package com.alipay.sofa.jraft.entity.codec;

import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.codec.v1.V1Decoder;
import com.alipay.sofa.jraft.entity.codec.v2.LogEntryV2CodecFactory;
import com.alipay.sofa.jraft.entity.codec.v2.V2Decoder;

/**
 * Decoder that supports both v1 and v2 log entry codec protocol.
 * @author boyan(boyan@antfin.com)
 *
 */
public class AutoDetectDecoder implements LogEntryDecoder {

    private AutoDetectDecoder() {

    }

    public static final AutoDetectDecoder INSTANCE = new AutoDetectDecoder();

    @Override
    public LogEntry decode(final byte[] bs) {
        if (bs == null || bs.length < 1) {
            return null;
        }

        if (bs[0] == LogEntryV2CodecFactory.MAGIC_BYTES[0]) {
            return V2Decoder.INSTANCE.decode(bs);
        } else {
            return V1Decoder.INSTANCE.decode(bs);
        }
    }

}
