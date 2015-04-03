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

package org.apache.hadoop.tools;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.JobSubmissionFiles;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.tools.util.DistCpTestUtils;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.tools.util.TestDistCpUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;

public class TestIntegrationByChunk {
  private static final Log LOG = LogFactory.getLog(TestIntegrationByChunk.class);

  private static FileSystem fs;

  private static Path listFile;
  private static Path target;
  private final static String SOURCE_PATH = "/tmp/source";
  private final static String LIST_PATH = "/tmp/listing";
  private final static String WORK_PATH = "/tmp/working";
  private final static String TARGET_PATH = "/tmp/target";
  private final static String ROOT_PATH = "/tmp";
  private static String root;
  private static final int DEFAULT_BUFFER_SIZE = 1024;
  private static final long DEFAULT_FILE_SIZE = 30*1024+900; //8 chunks
  private static final long MID_FILE_SIZE = 32*1024;     //8 chunks
  private static final long LARGE_FILE_SIZE = 320 *1024+76;  //81 chunks
  private static final long NON_DEFAULT_BLOCK_SIZE = 4*1024;

  private static MiniDFSCluster cluster;

  private static Configuration configuration;

  @BeforeClass
  public static void setup() throws Exception {
    try{
      configuration = getConfigurationForCluster();
      cluster = new MiniDFSCluster.Builder(configuration)
              .numDataNodes(1)
              .format(true)
              .build();
      fs = cluster.getFileSystem();
      // TODO: only for debuging unit, remove this when ok
      org.apache.log4j.LogManager.getRootLogger().setLevel(Level.DEBUG);
      listFile = new Path(LIST_PATH).makeQualified(fs.getUri(),
              fs.getWorkingDirectory());
      target = new Path(TARGET_PATH).makeQualified(fs.getUri(),
              fs.getWorkingDirectory());
      root = new Path(ROOT_PATH).makeQualified(fs.getUri(),
              fs.getWorkingDirectory()).toString();

      TestDistCpUtils.delete(fs, root);
  } catch (IOException e) {
      LOG.error("Exception encountered ", e);
    }
  }

  private static Configuration getConfigurationForCluster() throws IOException {
    Configuration configuration = new Configuration();
    System.setProperty("test.build.data", "target/tmp/build/TEST_COPY_CHUNK_MAPPER/data");
    configuration.set("hadoop.log.dir", "target/tmp");
    configuration.set("dfs.namenode.fs-limits.min-block-size", "0");
    configuration.set("dfs.blocksize",NON_DEFAULT_BLOCK_SIZE+"");
    LOG.debug("fs.default.name  == " + configuration.get("fs.default.name"));
    LOG.debug("dfs.http.address == " + configuration.get("dfs.http.address"));
    return configuration;
  }

  private static Configuration getConf() throws IOException {
    Configuration configuration = getConfigurationForCluster();
    final FileSystem fs = cluster.getFileSystem();
    Path workPath = new Path(WORK_PATH)
            .makeQualified(fs.getUri(), fs.getWorkingDirectory());
    Path targetPath = new Path(TARGET_PATH)
            .makeQualified(fs.getUri(), fs.getWorkingDirectory());
    configuration.set(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH,
            workPath.toString());
    configuration.set(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH,
            targetPath.toString());
    configuration.setBoolean(DistCpOptionSwitch.OVERWRITE.getConfigLabel(),
            false);
    configuration.setBoolean(DistCpOptionSwitch.SKIP_CRC.getConfigLabel(),
            false);
    configuration.setBoolean(DistCpOptionSwitch.SYNC_FOLDERS.getConfigLabel(),
            true);
    configuration.set(DistCpOptionSwitch.PRESERVE_STATUS.getConfigLabel(),
            "br");
    configuration.setBoolean(DistCpConstants.CONF_LABEL_COPY_BY_CHUNK, true);
    return configuration;
  }

  @Test(timeout=100000)
  public void testSingleFileMissingTargetByChunk(){
    caseSingleFileMissingTargetByChunk(false);
    System.out.println("split***********************************");
    //TODO sync is  controled by "-update"
    caseSingleFileMissingTargetByChunk(true);
  }

  /**
   * bychunk need FileSytem support concat operation
   * @param sync
   */
  private void caseSingleFileMissingTargetByChunk(boolean sync){
    try {
      addEntries(listFile, "singlefile1/file1");
      //createFiles("singlefile1/file1");
      long size = 2048+900; // 31*1024+900
      short replic = 2;
      long blockSize = 1024; // 32 chunks
      Path sourceFile = new Path(root + "/" + "singlefile1/file1");

      TestDistCpUtils.createFile(fs,sourceFile,size,replic,blockSize);
      Assert.assertEquals(blockSize,fs.getFileStatus(sourceFile).getBlockSize()); // assert blockSize
      DistCpOptions options = new DistCpOptions(listFile, target);
      options.setSyncFolder(sync);
      options.setDeleteMissing(false);

      //TODO: overwrite
      options.setOverwrite(false);
      options.setByChunk(true);
      options.setTargetPathExists(false);// forTargetMissing, rootPath is the sourcePath itself
      options.setPreserveStatus("bc");
      runTest(options);

      checkResult(options,target, 1,"singlefile1/file1");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test(timeout=100000)
  public void testSingleFileTargetFileByChunk() {
    caseSingleFileTargetFileByChunk(false);
    caseSingleFileTargetFileByChunk(true);
  }

  private void caseSingleFileTargetFileByChunk(boolean sync) {

    try {
      addEntries(listFile, "singlefile1/file1");
      createFilesWithDiffSeed("singlefile1/file1", "target");
      //createLargeFiles("singlefile1/file1", "target");
      DistCpOptions options = new DistCpOptions(listFile, target);
      options.setSyncFolder(sync);
      options.setDeleteMissing(false);
      options.setOverwrite(false);
      options.setByChunk(true);
      options.setTargetPathExists(true);
      options.setPreserveStatus("bc");

      runTest(options);

      checkResult(options,target, 1,"singlefile1/file1");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test(timeout=100000)
  public void testSingleFileTargetDirByChunk() {
    caseSingleFileTargetDirByChunk(false);
    caseSingleFileTargetDirByChunk(true);
  }

  private void caseSingleFileTargetDirByChunk(boolean sync) {

    try {
      addEntries(listFile, "singlefile2/file2");
      createFilesWithDiffSeed("singlefile2/file2");
      mkdirs(target.toString());

      DistCpOptions options = new DistCpOptions(listFile, target);
      options.setSyncFolder(sync);
      options.setDeleteMissing(false);
      options.setOverwrite(false);
      options.setByChunk(true);
      options.setTargetPathExists(true);
      options.setPreserveStatus("bc");
      runTest(options);

      checkResult(options,target, 1, "singlefile2/file2");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test(timeout=100000)
  public void testSingleDirTargetMissingByChunk() {
    caseSingleDirTargetMissingByChunk(false);
    caseSingleDirTargetMissingByChunk(true);
  }

  private void caseSingleDirTargetMissingByChunk(boolean sync) {

    try {
      addEntries(listFile, "singledir");
      mkdirs(root + "/singledir/dir1");

      DistCpOptions options = new DistCpOptions(listFile, target);
      options.setSyncFolder(sync);
      options.setDeleteMissing(false);
      options.setOverwrite(false);
      options.setByChunk(true);
      options.setTargetPathExists(false);
      options.setPreserveStatus("bc");
      runTest(options);

      checkResult(options,target, 1, "singledir");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test(timeout=100000)
  public void testSingleDirTargetPresentByChunk() {

    try {
      addEntries(listFile, "singledir");
      mkdirs(root + "/singledir/dir1");
      mkdirs(target.toString());

      DistCpOptions options = new DistCpOptions(listFile, target);
      options.setSyncFolder(false);
      options.setDeleteMissing(false);
      options.setOverwrite(false);
      options.setByChunk(true);
      options.setTargetPathExists(true);
      options.setPreserveStatus("bc");
      runTest(options);

      checkResult(options, target, 1, "singledir");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test(timeout=100000)
  public void testUpdateSingleDirTargetPresentByChunk() {

    try {
      addEntries(listFile, "Usingledir");
      mkdirs(root + "/Usingledir/Udir1");
      mkdirs(target.toString());

      DistCpOptions options = new DistCpOptions(listFile, target);
      options.setSyncFolder(true);
      options.setDeleteMissing(false);
      options.setOverwrite(false);
      options.setByChunk(true);
      options.setTargetPathExists(true);
      options.setPreserveStatus("bc");
      runTest(options);

      checkResult(options, target, 1, "Usingledir");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test(timeout=1000000)
  public void testMultiFileTargetPresentByChunk() {
    caseMultiFileTargetPresentByChunk(false);
    caseMultiFileTargetPresentByChunk(true);
  }

  private void caseMultiFileTargetPresentByChunk(boolean sync) {

    try {
      addEntries(listFile, "multifile/file3", "multifile/file4", "multifile/file5");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");
      //createLargeFiles("multifile/file3", "multifile/file4", "multifile/file5");
      mkdirs(target.toString());

      DistCpOptions options = new DistCpOptions(listFile, target);
      options.setSyncFolder(sync);
      options.setDeleteMissing(false);
      options.setOverwrite(false);
      options.setByChunk(true);
      options.setTargetPathExists(true);
      options.setPreserveStatus("bc");
      runTest(options);

      checkResult(options,target, 3, "multifile/file3", "multifile/file4", "multifile/file5");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test(timeout=100000)
  public void testCustomCopyListingByChunk() {

    try {

      addEntries(listFile, "multifile1/file3", "multifile1/file4", "multifile1/file5");
      createFiles("multifile1/file3", "multifile1/file4", "multifile1/file5");
      mkdirs(target.toString());

      Configuration conf = getConf();
      try {
        //exclude file3
        conf.setClass(DistCpConstants.CONF_LABEL_COPY_LISTING_CLASS,
            CustomCopyListing.class, CopyListing.class);
        DistCpOptions options = new DistCpOptions(Arrays.
            asList(new Path(root + "/" + "multifile1")), target);
        options.setSyncFolder(true);
        options.setDeleteMissing(false);
        options.setOverwrite(false);
        options.setByChunk(true);
        options.setPreserveStatus("bc");

        try {
          new DistCp(conf, options).execute();
          checkResult(options, target, 2, "multifile1/file4", "multifile1/file5");
        } catch (Exception e) {
          LOG.error("Exception encountered ", e);
          throw new IOException(e);
        }
      } finally {
        conf.unset(DistCpConstants.CONF_LABEL_COPY_LISTING_CLASS);
      }
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  private static class CustomCopyListing extends SimpleCopyListing {

    public CustomCopyListing(Configuration configuration,
                             Credentials credentials) {
      super(configuration, credentials);
    }

    @Override
    protected boolean shouldCopy(Path path, DistCpOptions options) {
      return !path.getName().equals("file3");
    }
  }

  @Test(timeout=100000)
  public void testMultiFileTargetMissingByChunk() {
    caseMultiFileTargetMissingByChunk(false);
    caseMultiFileTargetMissingByChunk(true);
  }

  private void caseMultiFileTargetMissingByChunk(boolean sync) {

    try {
      addEntries(listFile, "multifile/file3", "multifile/file4", "multifile/file5");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");

      DistCpOptions options = new DistCpOptions(listFile, target);
      options.setSyncFolder(sync);
      options.setDeleteMissing(false);
      options.setOverwrite(false);
      options.setByChunk(true);
      options.setTargetPathExists(false);
      options.setPreserveStatus("bc");
      runTest(options);

      checkResult(options, target, 3, "multifile/file3", "multifile/file4", "multifile/file5");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test(timeout=100000)
  public void testMultiDirTargetPresentByChunk() {

    try {
      addEntries(listFile, "multifile", "singledir");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");
      mkdirs(target.toString(), root + "/singledir/dir1");

      DistCpOptions options = new DistCpOptions(listFile, target);
      options.setSyncFolder(false);
      options.setDeleteMissing(false);
      options.setOverwrite(false);
      options.setByChunk(true);
      options.setTargetPathExists(true);
      options.setPreserveStatus("bc");
      runTest(options);

      checkResult(options, target, 2, "multifile", "singledir");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test(timeout=100000)
  public void testUpdateMultiDirTargetPresentByChunk() {

    try {
      addEntries(listFile, "Umultifile", "Usingledir");
      createFiles("Umultifile/Ufile3", "Umultifile/Ufile4", "Umultifile/Ufile5");
      mkdirs(target.toString(), root + "/Usingledir/Udir1");

      DistCpOptions options = new DistCpOptions(listFile, target);
      options.setSyncFolder(true);// update
      options.setDeleteMissing(false);
      options.setOverwrite(false);
      options.setByChunk(true);
      options.setTargetPathExists(true);
      options.setPreserveStatus("bc");
      runTest(options);

      checkResult(options, target, 4, "Umultifile", "Usingledir");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test(timeout=100000)
  public void testMultiDirTargetMissingByChunk() {

    try {
      addEntries(listFile, "multifile", "singledir");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");
      mkdirs(root + "/singledir/dir1");

      DistCpOptions options = new DistCpOptions(listFile, target);
      options.setSyncFolder(false);
      options.setDeleteMissing(false);
      options.setOverwrite(false);
      options.setByChunk(true);
      options.setTargetPathExists(false);
      options.setPreserveStatus("bc");
      runTest(options);

      checkResult(options, target, 2, "multifile", "singledir");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }

  @Test(timeout=100000)
  public void testUpdateMultiDirTargetMissingByChunk() {

    try {
      addEntries(listFile, "multifile", "singledir");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");
      mkdirs(root + "/singledir/dir1");

      DistCpOptions options = new DistCpOptions(listFile, target);
      options.setSyncFolder(true);
      options.setDeleteMissing(false);
      options.setOverwrite(false);
      options.setByChunk(true);
      options.setTargetPathExists(false);
      options.setPreserveStatus("bc");
      runTest(options);

      checkResult(options,target, 4, "multifile", "singledir");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
    }
  }
  
  @Test(timeout=100000)
  public void testDeleteMissingInDestinationByChunk() {
    
    try {
      addEntries(listFile, "srcdir");
      createFiles("srcdir/file1", "dstdir/file1", "dstdir/file2");
      
      Path dstTarget = new Path(root + "/dstdir");
      DistCpOptions options = new DistCpOptions(listFile, dstTarget);
      options.setSyncFolder(true);
      options.setDeleteMissing(true);
      options.setOverwrite(false);
      options.setTargetPathExists(false);
      options.setByChunk(true);
      runTest(options);
      
      checkResult(options,dstTarget, 1, "srcdir");
    } catch (IOException e) {
      LOG.error("Exception encountered while running distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
      TestDistCpUtils.delete(fs, "target/tmp1");
    }
  }
  
  @Test(timeout=100000)
  public void testOverwriteByChunk() {
    byte[] contents1 = "contents1".getBytes();
    byte[] contents2 = "contents2".getBytes();
    Assert.assertEquals(contents1.length, contents2.length);
    
    try {
      addEntries(listFile, "srcdir");
      createWithContents("srcdir/file1", contents1);
      createWithContents("dstdir/file1", contents2);
      
      Path dstTarget = new Path(root + "/dstdir");
      DistCpOptions options = new DistCpOptions(listFile, dstTarget);
      options.setSyncFolder(false);
      options.setDeleteMissing(false);
      options.setOverwrite(true);
      options.setTargetPathExists(false);
      options.setByChunk(true);
      runTest(options);
      
      checkResult(options,dstTarget, 1, "srcdir");
      
      // make sure dstdir/file1 has been overwritten with the contents
      // of srcdir/file1
      FSDataInputStream is = fs.open(new Path(root + "/dstdir/file1"));
      byte[] dstContents = new byte[contents1.length];
      is.readFully(dstContents);
      is.close();
      Assert.assertArrayEquals(contents1, dstContents);
    } catch (IOException e) {
      LOG.error("Exception encountered while running distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
      TestDistCpUtils.delete(fs, "target/tmp1");
    }
  }

  @Test(timeout=100000)
  public void testGlobTargetMissingSingleLevelByChunk() {

    try {
      Path listFile = new Path("target/tmp1/listing").makeQualified(fs.getUri(),
                                fs.getWorkingDirectory());
      addEntries(listFile, "*");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");
      createFiles("singledir/dir2/file6");

      DistCpOptions options = new DistCpOptions(listFile, target);
      options.setSyncFolder(false);
      options.setDeleteMissing(false);
      options.setOverwrite(false);
      options.setTargetPathExists(false);
      options.setByChunk(true);
      runTest(options);

      checkResult(target, 2, "multifile/file3", "multifile/file4", "multifile/file5",
          "singledir/dir2/file6");
    } catch (IOException e) {
      LOG.error("Exception encountered while testing distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
      TestDistCpUtils.delete(fs, "target/tmp1");
    }
  }

  @Test(timeout=100000)
  public void testUpdateGlobTargetMissingSingleLevelByChunk() {

    try {
      Path listFile = new Path("target/tmp1/listing").makeQualified(fs.getUri(),
                                  fs.getWorkingDirectory());
      addEntries(listFile, "*");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");
      createFiles("singledir/dir2/file6");

      DistCpOptions options = new DistCpOptions(listFile, target);
      options.setSyncFolder(true);
      options.setDeleteMissing(false);
      options.setOverwrite(false);
      options.setTargetPathExists(false);
      options.setByChunk(true);
      runTest(options);

      checkResult(target, 4, "file3", "file4", "file5", "dir2/file6");
    } catch (IOException e) {
      LOG.error("Exception encountered while running distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
      TestDistCpUtils.delete(fs, "target/tmp1");
    }
  }

  @Test(timeout=100000)
  public void testGlobTargetMissingMultiLevelByChunk() {

    try {
      Path listFile = new Path("target/tmp1/listing").makeQualified(fs.getUri(),
              fs.getWorkingDirectory());
      addEntries(listFile, "*/*");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");
      createFiles("singledir1/dir3/file7", "singledir1/dir3/file8",
          "singledir1/dir3/file9");

      DistCpOptions options = new DistCpOptions(listFile, target);
      options.setSyncFolder(false);
      options.setDeleteMissing(false);
      options.setOverwrite(false);
      options.setTargetPathExists(false);
      options.setByChunk(true);
      runTest(options);

      checkResult(target, 4, "file3", "file4", "file5",
          "dir3/file7", "dir3/file8", "dir3/file9");
    } catch (IOException e) {
      LOG.error("Exception encountered while running distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
      TestDistCpUtils.delete(fs, "target/tmp1");
    }
  }

  @Test(timeout=100000)
  public void testUpdateGlobTargetMissingMultiLevelByChunk() {

    try {
      Path listFile = new Path("target/tmp1/listing").makeQualified(fs.getUri(),
              fs.getWorkingDirectory());
      addEntries(listFile, "*/*");
      createFiles("multifile/file3", "multifile/file4", "multifile/file5");
      createFiles("singledir1/dir3/file7", "singledir1/dir3/file8",
          "singledir1/dir3/file9");
      DistCpOptions options = new DistCpOptions(listFile, target);
      options.setSyncFolder(true);
      options.setDeleteMissing(false);
      options.setOverwrite(false);
      options.setTargetPathExists(false);
      options.setByChunk(true);
      runTest(options);

      checkResult(target, 6, "file3", "file4", "file5",
          "file7", "file8", "file9");
    } catch (IOException e) {
      LOG.error("Exception encountered while running distcp", e);
      Assert.fail("distcp failure");
    } finally {
      TestDistCpUtils.delete(fs, root);
      TestDistCpUtils.delete(fs, "target/tmp1");
    }
  }
  
  @Test(timeout=100000)
  public void testCleanup() {
    try {
      Path sourcePath = new Path("noscheme:///file");
      List<Path> sources = new ArrayList<Path>();
      sources.add(sourcePath);

      DistCpOptions options = new DistCpOptions(sources, target);

      Configuration conf = getConf();
      Path stagingDir = JobSubmissionFiles.getStagingDir(
              new Cluster(conf), conf);
      stagingDir.getFileSystem(conf).mkdirs(stagingDir);

      try {
        new DistCp(conf, options).execute();
      } catch (Throwable t) {
        Assert.assertEquals(stagingDir.getFileSystem(conf).
            listStatus(stagingDir).length, 0);
      }
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("testCleanup failed " + e.getMessage());
    }
  }
  
  private void addEntries(Path listFile, String... entries) throws IOException {
    OutputStream out = fs.create(listFile);
    try {
      for (String entry : entries){
        out.write((root + "/" + entry).getBytes());
        out.write("\n".getBytes());
      }
    } finally {
      out.close();
    }
  }

  private void createFiles(String... entries) throws IOException {
    for (int i =0; i < entries.length;i++){
      TestDistCpUtils.delete(fs, root + "/" + entries[i]);
      touchFile(root + "/" + entries[i], DEFAULT_FILE_SIZE, false, new Options.ChecksumOpt(DataChecksum.Type.CRC32,
              512));
    }
  }

  private void createFilesWithDiffSeed(String... entries) throws IOException {
    for (int i =0; i < entries.length;i++){
      TestDistCpUtils.delete(fs, root + "/" + entries[i]);
      touchFile(root + "/" + entries[i], DEFAULT_FILE_SIZE, false, new Options.ChecksumOpt(DataChecksum.Type.CRC32,
              512),i);
    }
  }

  private void createLargeFiles(String... entries) throws IOException {
    for (int i =0; i < entries.length;i++){
      TestDistCpUtils.delete(fs, root + "/" + entries[i]);
      touchFile(root + "/" + entries[i], LARGE_FILE_SIZE, false, new Options.ChecksumOpt(DataChecksum.Type.CRC32,
              512),i);
    }
  }

  private static void touchFile(String path, long totalFileSize, boolean preserveBlockSize,
                                Options.ChecksumOpt checksumOpt) throws IOException {
    touchFile(path, totalFileSize, preserveBlockSize, checksumOpt,1);
  }

  private static void touchFile(String path, long totalFileSize, boolean preserveBlockSize,
                                Options.ChecksumOpt checksumOpt,long seed) throws IOException {
    FileSystem fs;
    DataOutputStream outputStream = null;
    try {
      fs = cluster.getFileSystem();
      final Path qualifiedPath = new Path(path).makeQualified(fs.getUri(),
              fs.getWorkingDirectory());
      final long blockSize = preserveBlockSize ? NON_DEFAULT_BLOCK_SIZE : fs
              .getDefaultBlockSize(qualifiedPath) * 2;
      FsPermission permission = FsPermission.getFileDefault().applyUMask(
              FsPermission.getUMask(fs.getConf()));
      outputStream = fs.create(qualifiedPath, permission,
              EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE), 0,
              (short) (fs.getDefaultReplication(qualifiedPath) * 2), blockSize,
              null, checksumOpt);
      try {

        if (totalFileSize > 0) {
          int bufferLen = DEFAULT_BUFFER_SIZE;
          byte[] toWrite = new byte[bufferLen];
          Random rb = new Random(seed);
          long bytesToWrite = totalFileSize;
          while (bytesToWrite>0) {
            rb.nextBytes(toWrite);
            int bytesToWriteNext = (bufferLen < bytesToWrite) ? bufferLen
                    : (int) bytesToWrite;

            outputStream.write(toWrite, 0, bytesToWriteNext);
            bytesToWrite -= bytesToWriteNext;
          }
        }
      } finally {
        if (outputStream != null) {
          outputStream.close();
        }
      }

      FileStatus fileStatus = fs.getFileStatus(qualifiedPath);
      System.out.println(fileStatus.getBlockSize());
      System.out.println(fileStatus.getReplication());
    }
    finally {
      IOUtils.cleanup(null, outputStream);
    }
  }
  
  private void createWithContents(String entry, byte[] contents) throws IOException {
    OutputStream out = fs.create(new Path(root + "/" + entry));
    try {
      out.write(contents);
    } finally {
      out.close();
    }
  }

  private void mkdirs(String... entries) throws IOException {
    for (String entry : entries){
      fs.mkdirs(new Path(entry));
    }
  }
    
  private void runTest(Path listFile, Path target, boolean targetExists,
      boolean sync) throws IOException {
    runTest(listFile, target, targetExists, sync, false, false);
  }

  private void runTest(Path listFile, Path target, boolean targetExists,
      boolean sync, boolean delete,
      boolean overwrite) throws IOException {
    DistCpOptions options = new DistCpOptions(listFile, target);
    options.setSyncFolder(sync);
    options.setDeleteMissing(delete);
    options.setOverwrite(overwrite);
    options.setTargetPathExists(targetExists);
    try {
      new DistCp(getConf(), options).execute();
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      throw new IOException(e);
    }
  }

  private void runTest(DistCpOptions options) throws IOException {
    try {
      new DistCp(getConf(), options).execute();
    } catch (Exception e) {
      LOG.error("Exception encountered ", e);
      throw new IOException(e);
    }
  }

  //for compatible
  private void checkResult(Path dstTarget, int count, String... relPaths) throws IOException {
    Assert.assertEquals(count, fs.listStatus(target).length);
    if (relPaths == null || relPaths.length == 0) {
      Assert.assertTrue(dstTarget.toString(), fs.exists(dstTarget));
      return;
    }
    boolean targetIsFile = fs.isFile(dstTarget);
    if(targetIsFile){
      //TODO: source must be one file
      validateFile(new Path(root+"/"+relPaths[0]),null,dstTarget,dstTarget,fs,fs);
    }
    else{
      //validate every sourcePath
      //TODO: for compatible
    }
  }

  private void checkResult(DistCpOptions options, Path dstTarget, int count, String... relPaths) throws IOException {
    Assert.assertEquals(count, fs.listStatus(dstTarget).length);
    if (relPaths == null || relPaths.length == 0) {
      Assert.assertTrue(dstTarget.toString(), fs.exists(dstTarget));
      return;
    }
    boolean targetIsFile = fs.isFile(dstTarget);
    if(targetIsFile){
      //TODO: source must be one file
      validateFile(new Path(root+"/"+relPaths[0]),null,dstTarget,dstTarget,fs,fs);
    }
    else{
      //validate every sourcePath
      for (String relPath : relPaths) {
        //TODO: Target Path
        Path source = new Path(root + "/" + relPath);
        Path sourceRoot = DistCpTestUtils.computeSourceRootPath(fs.getFileStatus(source),fs,fs,options);
        validateFile(source,sourceRoot, new Path(dstTarget.toString()+"/"+DistCpUtils.getRelativePath(sourceRoot,source)),dstTarget,fs, fs);
      }
    }
  }

  /**
   * TODO : checksum
   * @param sourcePath
   * @param targetPath
   * @param srcFS
   * @param targetFS
   * @return
   */
  private void validateFile(Path sourcePath, Path sourceRoot, Path targetPath,Path targetRoot, FileSystem srcFS, FileSystem targetFS) throws IOException{
    Assert.assertTrue(targetPath.toString(), fs.exists(targetPath));
    FileStatus targetFileStatus =  srcFS.getFileStatus(targetPath);
    FileStatus srcFileStatus = targetFS.getFileStatus(sourcePath);
    if(srcFileStatus.isDirectory()){
      // TODO: check dir recurisely
      FileStatus[] paths = fs.listStatus(sourcePath);
      for(FileStatus fileStatus: paths){
        validateFile(fileStatus.getPath(),sourceRoot,new Path(targetRoot.toString()+"/"+DistCpUtils.getRelativePath(sourceRoot, fileStatus.getPath())),targetRoot,srcFS,targetFS);
      }
    }else {
      // check file length
      Assert.assertEquals(srcFileStatus.getLen(),targetFileStatus.getLen());

      // TODO: check file by content
      long fileLen = srcFileStatus.getLen();
      long bufferLen = 8*1024;
      byte[] ori_content = new byte[(int)bufferLen];
      byte[] trg_content = new byte[(int)bufferLen];
      FSDataInputStream ori_stm;
      ori_stm = srcFS.open(srcFileStatus.getPath());
      FSDataInputStream trg_stm;
      trg_stm = targetFS.open(targetFileStatus.getPath());
      try {
        long position = 0;
        long readLen = 0;
        while (position < fileLen) {
          // ori_content
          if(fileLen-position<bufferLen){
            readLen = fileLen - position;
            ori_content = new byte[(int)readLen];
            trg_content = new byte[(int)readLen];
          }else{
            readLen = bufferLen;
          }
          ori_stm.readFully(position, ori_content);
          trg_stm.readFully(position, trg_content);
          for(int j = 0;j<readLen;j++){
            if(ori_content[j]!=trg_content[j]) {
              Assert.fail("content mismatch");
            }
          }
          position+=readLen;
        }
      }
      finally {
        ori_stm.close();
        trg_stm.close();
      }

      // TODO: check File by checksum

      Configuration conf = getConf();
      FileSystem sourceFS = sourcePath.getFileSystem(conf);
      String fileAttibutesStr = conf.get(DistCpConstants.CONF_LABEL_PRESERVE_STATUS,"");
      EnumSet<DistCpOptions.FileAttribute> fileAttributes = DistCpUtils.unpackAttributes(fileAttibutesStr);
      final FileChecksum sourceChecksum = fileAttributes
              .contains(DistCpOptions.FileAttribute.CHECKSUMTYPE) ? sourceFS
              .getFileChecksum(sourcePath) : null;
      if (!conf.getBoolean(DistCpConstants.CONF_LABEL_SKIP_CRC,false)) {
        Assert.assertEquals(true,DistCpUtils.compareCheckSums(sourceFS, srcFileStatus.getPath(), sourceChecksum,
                targetFS, targetPath));
      }
      LOG.info("source=("+srcFileStatus+"), target=("+targetFileStatus+")");
    }
  }
}
