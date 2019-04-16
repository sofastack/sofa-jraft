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
package com.alipay.sofa.jraft.storage.io;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.sofa.jraft.rpc.ProtobufMsgFactory;
import com.alipay.sofa.jraft.util.Bits;
import com.google.protobuf.Message;

/**
 * A file to store protobuf message. Format:
 * <ul>
 * <li>class name length(4 bytes)</li>
 * <li>class name</li>
 * <li> msg length(4 bytes)</li>
 * <li>msg data</li>
 * </ul>
 * @author boyan (boyan@alibaba-inc.com)
 *
 * 2018-Mar-12 8:56:23 PM
 */
public class ProtoBufFile {

    private static final Logger LOG = LoggerFactory.getLogger(ProtoBufFile.class);

    static {
        ProtobufMsgFactory.load();
    }

    /** file path */
    private final String        path;

    public ProtoBufFile(final String path) {
        this.path = path;
    }

    /**
     * Load a protobuf message from file.
     */
    public <T extends Message> T load() throws IOException {
        File file = new File(this.path);

        if (!file.exists()) {
            return null;
        }

        byte[] lenBytes = new byte[4];
        try (FileInputStream fin = new FileInputStream(file); BufferedInputStream input = new BufferedInputStream(fin)) {
            readBytes(lenBytes, input);
            int len = Bits.getInt(lenBytes, 0);
            if (len <= 0) {
                throw new IOException("Invalid message fullName.");
            }
            byte[] nameBytes = new byte[len];
            readBytes(nameBytes, input);
            String name = new String(nameBytes);
            readBytes(lenBytes, input);
            int msgLen = Bits.getInt(lenBytes, 0);
            byte[] msgBytes = new byte[msgLen];
            readBytes(msgBytes, input);
            return ProtobufMsgFactory.newMessageByProtoClassName(name, msgBytes);
        }
    }

    private void readBytes(final byte[] bs, final InputStream input) throws IOException {
        int read;
        if ((read = input.read(bs)) != bs.length) {
            throw new IOException("Read error, expects " + bs.length + " bytes, but read " + read);
        }
    }

    /**
     * Save a protobuf message to file.
     *
     * @param msg   protobuf message
     * @param sync  if sync flush data to disk
     * @return      true if save success
     */
    public boolean save(final Message msg, final boolean sync) throws IOException {
        // Write message into temp file
        File file = new File(this.path + ".tmp");
        try (FileOutputStream fOut = new FileOutputStream(file);
                BufferedOutputStream output = new BufferedOutputStream(fOut)) {
            byte[] lenBytes = new byte[4];

            // name len + name
            String fullName = msg.getDescriptorForType().getFullName();
            int nameLen = fullName.length();
            Bits.putInt(lenBytes, 0, nameLen);
            output.write(lenBytes);
            output.write(fullName.getBytes());
            // msg len + msg
            int msgLen = msg.getSerializedSize();
            Bits.putInt(lenBytes, 0, msgLen);
            output.write(lenBytes);
            msg.writeTo(output);
            if (sync) {
                fOut.getFD().sync();
            }
        }

        // Move temp file to target path atomically.
        // The code comes from https://github.com/jenkinsci/jenkins/blob/master/core/src/main/java/hudson/util/AtomicFileWriter.java#L187
        Path tmpPath = file.toPath();
        File destFile = new File(this.path);
        Path destPath = destFile.toPath();
        try {
            return Files.move(tmpPath, destPath, StandardCopyOption.ATOMIC_MOVE) != null;
        } catch (final IOException e) {
            // If it falls here that can mean many things. Either that the atomic move is not supported,
            // or something wrong happened. Anyway, let's try to be over-diagnosing
            if (e instanceof AtomicMoveNotSupportedException) {
                LOG.warn("Atomic move not supported. falling back to non-atomic move, error: {}.", e.getMessage());
            } else {
                LOG.warn("Unable to move atomically, falling back to non-atomic move, error: {}.", e.getMessage());
            }

            if (destFile.exists()) {
                LOG.info("The target file {} was already existing.", destPath);
            }

            try {
                return Files.move(tmpPath, destPath, StandardCopyOption.REPLACE_EXISTING) != null;
            } catch (final IOException e1) {
                e1.addSuppressed(e);
                LOG.warn("Unable to move {} to {}. Attempting to delete {} and abandoning.", tmpPath, destPath, tmpPath);
                try {
                    Files.deleteIfExists(tmpPath);
                } catch (IOException e2) {
                    e2.addSuppressed(e1);
                    LOG.warn("Unable to delete {}, good bye then!", tmpPath);
                    throw e2;
                }

                throw e1;
            }
        }
    }
}
