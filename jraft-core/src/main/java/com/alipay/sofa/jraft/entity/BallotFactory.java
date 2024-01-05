/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.entity;

import com.alipay.sofa.jraft.Quorum;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.codec.v2.LogOutter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Objects;

/**
 * @author Akai
 */
public final class BallotFactory {
    private static final Logger     LOG                  = LoggerFactory.getLogger(BallotFactory.class);
    private static final String     defaultDecimalFactor = "0.1";
    private static final BigDecimal defaultDecimal       = new BigDecimal(defaultDecimalFactor);

    public static Quorum buildFlexibleQuorum(int readFactor, int writeFactor, int size) {
        // if size equals 0,config must be empty,so we just return null
        if (size == 0) {
            return null;
        }
        // Check if factors are valid
        if (!checkValid(readFactor, writeFactor)) {
            LOG.error("Invalid factor, factor's range must be (0,10) and the sum of factor should be 10");
            return null;
        }
        // Calculate quorum
        int w = calculateWriteQuorum(writeFactor, size);
        int r = calculateReadQuorum(readFactor, size);
        return new Quorum(w, r);
    }

    public static Quorum buildMajorityQuorum(int size) {
        // if size equals 0,config must be empty,so we just return null
        if (size == 0) {
            return null;
        }
        int majorityQuorum = calculateMajorityQuorum(size);
        return new Quorum(majorityQuorum, majorityQuorum);
    }

    private static int calculateWriteQuorum(int writeFactor, int n) {
        BigDecimal writeFactorDecimal = defaultDecimal.multiply(new BigDecimal(writeFactor))
            .multiply(new BigDecimal(n));
        return writeFactorDecimal.setScale(0, RoundingMode.CEILING).intValue();
    }

    private static int calculateReadQuorum(int readFactor, int n) {
        int writeQuorum = calculateWriteQuorum(10 - readFactor, n);
        return n - writeQuorum + 1;
    }

    private static int calculateMajorityQuorum(int n) {
        return n / 2 + 1;
    }

    public static boolean checkValid(int readFactor, int writeFactor) {
        if (readFactor == 0 && writeFactor == 0) {
            LOG.error("When turning on flexible mode, Both of readFactor and writeFactor should not be 0.");
            return false;
        }
        if (readFactor + writeFactor == 10 && readFactor > 0 && readFactor < 10 && writeFactor > 0 && writeFactor < 10) {
            return true;
        }
        LOG.error("Fail to set quorum_nwr because the sum of read_factor and write_factor is {} , not 10",
            readFactor + writeFactor);
        return false;
    }

    public static LogEntry convertConfigToLogEntry(LogEntry logEntry, Configuration conf) {
        if (Objects.isNull(logEntry)) {
            logEntry = new LogEntry();
        }
        logEntry.setEnableFlexible(false);
        logEntry.setPeers(conf.listPeers());
        final LogOutter.Quorum.Builder quorumBuilder = LogOutter.Quorum.newBuilder();
        LogOutter.Quorum quorum = quorumBuilder.setR(conf.getQuorum().getR()).setW(conf.getQuorum().getW()).build();
        logEntry.setQuorum(quorum);
        return logEntry;
    }

    public static LogEntry convertOldConfigToLogOuterEntry(LogEntry logEntry, Configuration conf) {
        if (Objects.isNull(logEntry)) {
            logEntry = new LogEntry();
        }
        logEntry.setEnableFlexible(false);
        logEntry.setOldPeers(conf.listPeers());
        final LogOutter.Quorum.Builder quorumBuilder = LogOutter.Quorum.newBuilder();
        LogOutter.Quorum quorum = quorumBuilder.setR(conf.getQuorum().getR()).setW(conf.getQuorum().getW()).build();
        logEntry.setOldQuorum(quorum);
        return logEntry;
    }
}
