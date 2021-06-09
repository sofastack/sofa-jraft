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
package com.alipay.sofa.jraft.rhea.storage.zip;

import com.alipay.sofa.jraft.util.CRC64;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.Checksum;

/**
 * @author hzh
 */
public class JDKZipStrategyTest {
    private File        sourceDir;

    private ZipStrategy zipStrategy;

    @Before
    public void startup() throws IOException {
        this.sourceDir = new File("zip_test");
        this.zipStrategy = new JDKZipStrategy();

        if (this.sourceDir.exists()) {
            FileUtils.forceDelete(this.sourceDir);
        }
        FileUtils.forceMkdir(this.sourceDir);
        final File f1 = Paths.get(this.sourceDir.getAbsolutePath(), "f1").toFile();
        FileUtils.write(f1, "f1");
        final File d1 = Paths.get(this.sourceDir.getAbsolutePath(), "d1").toFile();
        FileUtils.forceMkdir(d1);
        final File f11 = Paths.get(d1.getAbsolutePath(), "f11").toFile();
        FileUtils.write(f11, "f11");

        final File d2 = Paths.get(d1.getAbsolutePath(), "d2").toFile();
        FileUtils.forceMkdir(d2);

        final File d3 = Paths.get(d2.getAbsolutePath(), "d3").toFile();
        FileUtils.forceMkdir(d3);

        final File f31 = Paths.get(d3.getAbsolutePath(), "f31").toFile();
        FileUtils.write(f31, "f32");

    }

    @After
    public void teardown() throws IOException {
        FileUtils.forceDelete(this.sourceDir);
    }

    @Test
    public void zipTest() throws Throwable {
        final String rootPath = this.sourceDir.toPath().toAbsolutePath().getParent().toString();
        final Path outPath = Paths.get(rootPath, "kv.zip");
        final Checksum c1 = new CRC64();
        zipStrategy.compress(rootPath, "zip_test", outPath.toString(), c1);

        System.out.println(Long.toHexString(c1.getValue()));

        final Checksum c2 = new CRC64();
        zipStrategy.deCompress(Paths.get(rootPath, "kv.zip").toString(), rootPath, c2);

        Assert.assertEquals(c1.getValue(), c2.getValue());

        FileUtils.forceDelete(outPath.toFile());
    }
}
