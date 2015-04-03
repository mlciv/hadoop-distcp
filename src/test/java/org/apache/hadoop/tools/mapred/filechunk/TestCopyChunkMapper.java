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

package org.apache.hadoop.tools.mapred.filechunk;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.*;
import org.apache.hadoop.tools.DistCp.Counter;
import org.apache.hadoop.tools.mapred.CopyCommitter;
import org.apache.hadoop.tools.mapred.CopyMapper;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;

public class TestCopyChunkMapper {
  private static final Log LOG = LogFactory.getLog(TestCopyChunkMapper.class);
  private static List<Path> pathList = new ArrayList<Path>();
  private static int nFiles = 0;
  private static final int DEFAULT_FILE_SIZE = 1024;
  private static final long MID_FILE_SIZE = 32*1024;     //8 chunks
  private static final long LARGE_FILE_SIZE = 321 *1024;  //81 chunks
  private static final long NON_DEFAULT_BLOCK_SIZE = 4*1024;

  private static MiniDFSCluster cluster;

  private static final String SOURCE_PATH = "/tmp/source";
  private static final String TARGET_PATH = "/tmp/target";

  private static Configuration configuration;

  @BeforeClass
  public static void setup() throws Exception {
    configuration = getConfigurationForCluster();
    cluster = new MiniDFSCluster.Builder(configuration)
                .numDataNodes(1)
                .format(true)
                .build();
    // TODO: only for debuging unit, remove this when ok
    org.apache.log4j.LogManager.getRootLogger().setLevel(Level.DEBUG);
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

  private static Configuration getConfiguration() throws IOException {
    Configuration configuration = getConfigurationForCluster();
    final FileSystem fs = cluster.getFileSystem();
    Path workPath = new Path(TARGET_PATH)
            .makeQualified(fs.getUri(), fs.getWorkingDirectory());
    configuration.set(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH,
            workPath.toString());
    configuration.set(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH,
            workPath.toString());
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

  private static void createSourceData() throws Exception {
    mkdirs(SOURCE_PATH + "/1");     //1
    mkdirs(SOURCE_PATH + "/2");     //1
    mkdirs(SOURCE_PATH + "/2/3/4"); //2
    mkdirs(SOURCE_PATH + "/2/3");   //0
    mkdirs(SOURCE_PATH + "/5");     //1
    touchFile(SOURCE_PATH + "/5/6",MID_FILE_SIZE,true,new ChecksumOpt(DataChecksum.Type.CRC32,
            512));                  //8
    mkdirs(SOURCE_PATH + "/7");     //1
    mkdirs(SOURCE_PATH + "/7/8");   //1
    touchFile(SOURCE_PATH + "/7/8/9",LARGE_FILE_SIZE,true,new ChecksumOpt(DataChecksum.Type.CRC32,
            512));                  //81
    //96 chunks
  }

  private static void appendSourceData() throws Exception {
    FileSystem fs = cluster.getFileSystem();
    for (Path source : pathList) {
      if (fs.getFileStatus(source).isFile()) {
        // append 2048 bytes per file
        appendFile(source, DEFAULT_FILE_SIZE * 2);
      }
    }
  }

  private static void createSourceDataWithDifferentBlockSize() throws Exception {
    mkdirs(SOURCE_PATH + "/1");
    mkdirs(SOURCE_PATH + "/2");
    mkdirs(SOURCE_PATH + "/2/3/4");
    mkdirs(SOURCE_PATH + "/2/3");
    mkdirs(SOURCE_PATH + "/5");
    touchFile(SOURCE_PATH + "/5/6", true, null);
    mkdirs(SOURCE_PATH + "/7");
    mkdirs(SOURCE_PATH + "/7/8");
    touchFile(SOURCE_PATH + "/7/8/9");
  }

  private static void createSourceDataWithDifferentChecksumType()
      throws Exception {
    mkdirs(SOURCE_PATH + "/1");
    mkdirs(SOURCE_PATH + "/2");
    mkdirs(SOURCE_PATH + "/2/3/4");
    mkdirs(SOURCE_PATH + "/2/3");
    mkdirs(SOURCE_PATH + "/5");
    touchFile(SOURCE_PATH + "/5/6", new ChecksumOpt(DataChecksum.Type.CRC32,
        512));
    mkdirs(SOURCE_PATH + "/7");
    mkdirs(SOURCE_PATH + "/7/8");
    touchFile(SOURCE_PATH + "/7/8/9", new ChecksumOpt(DataChecksum.Type.CRC32C,
        512));
  }

  private static void mkdirs(String path) throws Exception {
    FileSystem fileSystem = cluster.getFileSystem();
    final Path qualifiedPath = new Path(path).makeQualified(fileSystem.getUri(),
                                              fileSystem.getWorkingDirectory());
    pathList.add(qualifiedPath);
    fileSystem.mkdirs(qualifiedPath);
  }

  private static void touchFile(String path) throws Exception {
    touchFile(path, false, null);
  }

  private static void touchFile(String path, ChecksumOpt checksumOpt)
      throws Exception {
    // create files with specific checksum opt and non-default block size
    touchFile(path, true, checksumOpt);
  }

  private static void touchFile(String path, boolean createMultipleBlocks,
      ChecksumOpt checksumOpt) throws Exception {
    FileSystem fs;
    DataOutputStream outputStream = null;
    try {
      fs = cluster.getFileSystem();
      final Path qualifiedPath = new Path(path).makeQualified(fs.getUri(),
          fs.getWorkingDirectory());
      final long blockSize = createMultipleBlocks ? NON_DEFAULT_BLOCK_SIZE : fs
          .getDefaultBlockSize(qualifiedPath) * 2;
      FsPermission permission = FsPermission.getFileDefault().applyUMask(
          FsPermission.getUMask(fs.getConf()));
      outputStream = fs.create(qualifiedPath, permission,
          EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE), 0,
          (short) (fs.getDefaultReplication(qualifiedPath) * 2), blockSize,
          null, checksumOpt);
      byte[] bytes = new byte[DEFAULT_FILE_SIZE];
      outputStream.write(bytes);
      long fileSize = DEFAULT_FILE_SIZE;
      if (createMultipleBlocks) {
        while (fileSize < 2*blockSize) {
          outputStream.write(bytes);
          outputStream.flush();
          fileSize += DEFAULT_FILE_SIZE;
        }
      }
      pathList.add(qualifiedPath);
      ++nFiles;

      FileStatus fileStatus = fs.getFileStatus(qualifiedPath);
      System.out.println(fileStatus.getBlockSize());
      System.out.println(fileStatus.getReplication());
    }
    finally {
      IOUtils.cleanup(null, outputStream);
    }
  }

  private static void touchFile(String path, long totalFileSize, boolean preserveBlockSize,
                                ChecksumOpt checksumOpt) throws Exception {
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
      byte[] bytes = new byte[DEFAULT_FILE_SIZE];
      long curFileSize = 0;
      int bufferLen = DEFAULT_FILE_SIZE;
      while (curFileSize < totalFileSize) {
        if(totalFileSize -curFileSize< DEFAULT_FILE_SIZE)
          bufferLen = (int)(totalFileSize - curFileSize);
        outputStream.write(bytes,0,bufferLen);
        outputStream.flush();
        curFileSize += bufferLen;
      }
      pathList.add(qualifiedPath);
      ++nFiles;

      FileStatus fileStatus = fs.getFileStatus(qualifiedPath);
      System.out.println(fileStatus.getBlockSize());
      System.out.println(fileStatus.getReplication());
    }
    finally {
      IOUtils.cleanup(null, outputStream);
    }
  }


  /**
   * Append specified length of bytes to a given file
   */
  private static void appendFile(Path p, int length) throws IOException {
    byte[] toAppend = new byte[length];
    Random random = new Random();
    random.nextBytes(toAppend);
    FSDataOutputStream out = cluster.getFileSystem().append(p);
    try {
      out.write(toAppend);
    } finally {
      IOUtils.closeStream(out);
    }
  }

  // TODO: checksum not supported yet for chunk
  public void testCopyWithDifferentChecksumType() throws Exception {
    testCopy(true);
  }

  @Test(timeout=40000)
  public void testRun() throws Exception {
    testCopy(false);
  }

  public void testCopy(boolean preserveChecksum) throws Exception {
    deleteState();
    if (preserveChecksum) {
      createSourceDataWithDifferentChecksumType();
    } else {
      createSourceData();
    }

    FileSystem fs = cluster.getFileSystem();
    CopyChunkMapper copyChunkMapper = new CopyChunkMapper();
    StubContext stubContext = new StubContext(getConfiguration(), null, 0);
    Mapper<Text, CopyListingChunkFileStatus, Text, Text>.Context context
            = stubContext.getContext();

    Configuration configuration = context.getConfiguration();
    EnumSet<DistCpOptions.FileAttribute> fileAttributes
            = EnumSet.of(DistCpOptions.FileAttribute.REPLICATION);
    if (preserveChecksum) {
      fileAttributes.add(DistCpOptions.FileAttribute.CHECKSUMTYPE);
    }
    configuration.set(DistCpOptionSwitch.PRESERVE_STATUS.getConfigLabel(),
            DistCpUtils.packAttributes(fileAttributes));
    mkdirs(TARGET_PATH);
    copyChunkMapper.setup(context);

    //TODO: to keep the same with configuration
    SimpleCopyListing listing = new SimpleCopyListing(configuration);
    DistCpOptions options = OptionsParser.parse(
            new String[]{
                    "-pb",
                    "-bychunk",
                    fs.getFileStatus(new Path(SOURCE_PATH)).getPath().toString(),
                    fs.getFileStatus(new Path(TARGET_PATH)).getPath().toString()}
    );
    options.setTargetPathExists(true);
    Assert.assertEquals(options.getByChunk(),true);
    Path listingFile = new Path("/tmp/chunkSeqList");
    listing.buildListing(listingFile, options);
    long numChunks = listing.getNumberOfPaths();
    Assert.assertEquals(81+8+7+1,numChunks);
    SequenceFile.Reader reader = new SequenceFile.Reader(configuration,
            SequenceFile.Reader.file(listingFile));
    if(options.getByChunk()) {
      CopyListingChunkFileStatus fileStatus = new CopyListingChunkFileStatus();
      Text relativePath = new Text();
      while (reader.next(relativePath, fileStatus)) {
        copyChunkMapper.map(relativePath,fileStatus,context);
      }
    }
    //TODO: verify every chunk


    //TODO Stitch chunks
    // Check that the maps worked.
    //verifyCopy(fs, preserveChecksum);
    Assert.assertEquals(numChunks, stubContext.getReporter()
        .getCounter(Counter.COPY).getValue());
    Assert.assertEquals(MID_FILE_SIZE+LARGE_FILE_SIZE,stubContext.getReporter().getCounter(Counter.BYTESCOPIED).getValue());

//    if (!preserveChecksum) {
//      Assert.assertEquals(nFiles * DEFAULT_FILE_SIZE, stubContext
//          .getReporter().getCounter(Counter.BYTESCOPIED)
//          .getValue());
//    } else {
//      Assert.assertEquals(nFiles * NON_DEFAULT_BLOCK_SIZE * 2, stubContext
//          .getReporter().getCounter(Counter.BYTESCOPIED)
//          .getValue());
//    }

    //TODO: test Skip
//    testCopyingExistingFiles(fs, copyChunkMapper, context);
//    for (Text value : stubContext.getWriter().values()) {
//      Assert.assertTrue(value.toString() + " is not skipped", value
//          .toString().startsWith("SKIP:"));
//    }
  }

  private void verifyCopy(FileSystem fs, boolean preserveChecksum)
      throws Exception {
    for (Path path : pathList) {
      final Path targetPath = new Path(path.toString().replaceAll(SOURCE_PATH,
          TARGET_PATH));
      Assert.assertTrue(fs.exists(targetPath));
      Assert.assertTrue(fs.isFile(targetPath) == fs.isFile(path));
      FileStatus sourceStatus = fs.getFileStatus(path);
      FileStatus targetStatus = fs.getFileStatus(targetPath);
      Assert.assertEquals(sourceStatus.getReplication(),
          targetStatus.getReplication());
      if (preserveChecksum) {
        Assert.assertEquals(sourceStatus.getBlockSize(),
            targetStatus.getBlockSize());
      }
      Assert.assertTrue(!fs.isFile(targetPath)
          || fs.getFileChecksum(targetPath).equals(fs.getFileChecksum(path)));
    }
  }

  private void doTestIgnoreFailures(boolean ignoreFailures) {
    try {
      deleteState();
      createSourceData();

      FileSystem fs = cluster.getFileSystem();
      CopyMapper copyMapper = new CopyMapper();
      StubContext stubContext = new StubContext(getConfiguration(), null, 0);
      Mapper<Text, CopyListingFileStatus, Text, Text>.Context context
              = stubContext.getContext();

      Configuration configuration = context.getConfiguration();
      configuration.setBoolean(
              DistCpOptionSwitch.IGNORE_FAILURES.getConfigLabel(),ignoreFailures);
      configuration.setBoolean(DistCpOptionSwitch.OVERWRITE.getConfigLabel(),
              true);
      configuration.setBoolean(DistCpOptionSwitch.SKIP_CRC.getConfigLabel(),
              true);
      copyMapper.setup(context);

      for (Path path : pathList) {
        final FileStatus fileStatus = fs.getFileStatus(path);
        if (!fileStatus.isDirectory()) {
          fs.delete(path, true);
          copyMapper.map(new Text(DistCpUtils.getRelativePath(new Path(SOURCE_PATH), path)),
                  new CopyListingFileStatus(fileStatus), context);
        }
      }
      if (ignoreFailures) {
        for (Text value : stubContext.getWriter().values()) {
          Assert.assertTrue(value.toString() + " is not skipped", value.toString().startsWith("FAIL:"));
        }
      }
      Assert.assertTrue("There should have been an exception.", ignoreFailures);
    }
    catch (Exception e) {
      Assert.assertTrue("Unexpected exception: " + e.getMessage(),
              !ignoreFailures);
      e.printStackTrace();
    }
  }

  private static void deleteState() throws IOException {
    pathList.clear();
    nFiles = 0;
    cluster.getFileSystem().delete(new Path(SOURCE_PATH), true);
    cluster.getFileSystem().delete(new Path(TARGET_PATH), true);
  }
}
