/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.tools.util;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.LAZY_PERSIST;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.util.ToolRunner;

/**
 * Utility class for DistCpTests
 */
public class DistCpTestUtils {

   /**
    * Asserts the XAttrs returned by getXAttrs for a specific path match an
    * expected set of XAttrs.
    *
    * @param path String path to check
    * @param fs FileSystem to use for the path
    * @param expectedXAttrs XAttr[] expected xAttrs
    * @throws Exception if there is any error
    */
  public static void assertXAttrs(Path path, FileSystem fs,
      Map<String, byte[]> expectedXAttrs)
      throws Exception {
    Map<String, byte[]> xAttrs = fs.getXAttrs(path);
    assertEquals(path.toString(), expectedXAttrs.size(), xAttrs.size());
    Iterator<Entry<String, byte[]>> i = expectedXAttrs.entrySet().iterator();
    while (i.hasNext()) {
      Entry<String, byte[]> e = i.next();
      String name = e.getKey();
      byte[] value = e.getValue();
      if (value == null) {
        assertTrue(xAttrs.containsKey(name) && xAttrs.get(name) == null);
      } else {
        assertArrayEquals(value, xAttrs.get(name));
      }
    }
  }


  public static Path computeSourceRootPath(FileStatus sourceStatus,FileSystem srcFS,
                                     FileSystem targetFS,DistCpOptions options) throws IOException {

    Path target = options.getTargetPath();
    final boolean targetPathExists = options.getTargetPathExists();

    boolean solitaryFile = options.getSourcePaths().size() == 1
            && !sourceStatus.isDirectory();

    if (solitaryFile) {
      if (targetFS.isFile(target) || !targetPathExists) {
        return sourceStatus.getPath();
      } else {
        return sourceStatus.getPath().getParent();
      }
    } else {
      boolean specialHandling = (options.getSourcePaths().size() == 1 && !targetPathExists) ||
              options.shouldSyncFolder() || options.shouldOverwrite();

      return specialHandling && sourceStatus.isDirectory() ? sourceStatus.getPath() :
              sourceStatus.getPath().getParent();
    }
  }

  /**
   * Runs distcp from src to dst, preserving XAttrs. Asserts the
   * expected exit code.
   *
   * @param exitCode expected exit code
   * @param src distcp src path
   * @param dst distcp destination
   * @param options distcp command line options
   * @param conf Configuration to use
   * @throws Exception if there is any error
   */
  public static void assertRunDistCp(int exitCode, String src, String dst,
      String options, Configuration conf)
      throws Exception {
    DistCp distCp = new DistCp(conf, null);
    String[] optsArr = options == null ?
        new String[] { src, dst } :
        new String[] { options, src, dst };
    assertEquals(exitCode,
        ToolRunner.run(conf, distCp, optsArr));
  }


  public static void createFile(FileSystem fs, Path fileName, long fileLen,
                                short replFactor, long seed) throws IOException {
    createFile(fs, fileName, 1024, fileLen, fs.getDefaultBlockSize(fileName),
            replFactor, seed);
  }

  public static void createFile(FileSystem fs, Path fileName, int bufferLen,
                                long fileLen, long blockSize, short replFactor, long seed)
          throws IOException {
    createFile(fs, fileName, false, bufferLen, fileLen, blockSize,
            replFactor, seed, false);
  }

  public static void createFile(FileSystem fs, Path fileName,
                                boolean isLazyPersist, int bufferLen, long fileLen, long blockSize,
                                short replFactor, long seed, boolean flush) throws IOException {
    assert bufferLen > 0;
    if (!fs.mkdirs(fileName.getParent())) {
      throw new IOException("Mkdirs failed to create " +
              fileName.getParent().toString());
    }
    FSDataOutputStream out = null;
    EnumSet<CreateFlag> createFlags = EnumSet.of(CREATE);
    createFlags.add(OVERWRITE);
    if (isLazyPersist) {
      createFlags.add(LAZY_PERSIST);
    }
    try {
      out = fs.create(fileName, FsPermission.getFileDefault(), createFlags,
              fs.getConf().getInt(CommonConfigurationKeys.IO_FILE_BUFFER_SIZE_KEY, 4096),
              replFactor, blockSize, null);

      if (fileLen > 0) {
        byte[] toWrite = new byte[bufferLen];
        Random rb = new Random(seed);
        long bytesToWrite = fileLen;
        while (bytesToWrite>0) {
          rb.nextBytes(toWrite);
          int bytesToWriteNext = (bufferLen < bytesToWrite) ? bufferLen
                  : (int) bytesToWrite;

          out.write(toWrite, 0, bytesToWriteNext);
          bytesToWrite -= bytesToWriteNext;
        }
        if (flush) {
          out.hsync();
        }
      }
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }
}
