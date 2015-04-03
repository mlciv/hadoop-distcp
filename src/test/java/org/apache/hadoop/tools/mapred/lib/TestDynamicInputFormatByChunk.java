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

package org.apache.hadoop.tools.mapred.lib;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.tools.*;
import org.apache.hadoop.tools.util.TestDistCpUtils;
import org.apache.hadoop.util.DataChecksum;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class TestDynamicInputFormatByChunk {
  private static final Log LOG = LogFactory.getLog(TestDynamicInputFormatByChunk.class);
  private static MiniDFSCluster cluster;
  private static final int N_FILES = 10;// too large
  private static final int NUM_SPLITS = 7;
  private static final int DEFAULT_FILE_SIZE=1024;
  private static final long MID_FILE_SIZE = 32*1024;     //8 chunks
  private static final long LARGE_FILE_SIZE = 321 *1024;  //81 chunks
  private static final long NON_DEFAULT_BLOCK_SIZE = 4*1024;

  private static final Credentials CREDENTIALS = new Credentials();

  private static List<String> expectedFilePaths = new ArrayList<String>(N_FILES);
  private static List<String> expectedChunkFilePaths = new ArrayList<String>();

  @BeforeClass
  public static void setup() throws Exception {
    cluster = new MiniDFSCluster.Builder(getConfigurationForCluster())
                  .numDataNodes(1).format(true).build();
    // TODO: only for debuging unit, remove this when ok
    //org.apache.log4j.LogManager.getRootLogger().setLevel(Level.DEBUG);
  }

  public static void prepareData() throws Exception{
    expectedFilePaths.clear();
    expectedChunkFilePaths.clear();
    for (int i=0; i<N_FILES; ++i)
      createFile("/tmp/source/" + String.valueOf(i));

    FileSystem fileSystem = cluster.getFileSystem();
    expectedFilePaths.add(fileSystem.listStatus(
            new Path("/tmp/source/0"))[0].getPath().getParent().toString());
    expectedChunkFilePaths.add(fileSystem.listStatus(
            new Path("/tmp/source/0"))[0].getPath().getParent().toString());
    //TODO: this expected Path only consider the file, not supported for chunk
  }

  private static Configuration getConfigurationForCluster() {
    Configuration configuration = new Configuration();
    System.setProperty("test.build.data",
                       "target/tmp/build/TEST_DYNAMIC_INPUT_FORMAT/data");
    configuration.set("hadoop.log.dir", "target/tmp");
    LOG.debug("fs.default.name  == " + configuration.get("fs.default.name"));
    LOG.debug("dfs.http.address == " + configuration.get("dfs.http.address"));
    configuration.set("dfs.blocksize",NON_DEFAULT_BLOCK_SIZE+"");
    return configuration;
  }

  private static DistCpOptions getOptions() throws Exception {
    Path sourcePath = new Path(cluster.getFileSystem().getUri().toString()
            + "/tmp/source");
    Path targetPath = new Path(cluster.getFileSystem().getUri().toString()
            + "/tmp/target");

    List<Path> sourceList = new ArrayList<Path>();
    sourceList.add(sourcePath);
    DistCpOptions options = new DistCpOptions(sourceList, targetPath);
    options.setMaxMaps(NUM_SPLITS);
    return options;
  }

  private static void createFile(String path) throws Exception {
    FileSystem fileSystem = null;
    try{
      fileSystem = cluster.getFileSystem();
      //outputStream = fileSystem.create(new Path(path), true, 0);
      touchFile(path, MID_FILE_SIZE, true, new Options.ChecksumOpt(DataChecksum.Type.CRC32,
              512));
      expectedFilePaths.add(fileSystem.listStatus(
                                    new Path(path))[0].getPath().toString());

      //adding to expectedChunkFilePaths
      for(int i=0;i<MID_FILE_SIZE/NON_DEFAULT_BLOCK_SIZE;i++){
        expectedChunkFilePaths.add(fileSystem.listStatus(
                new Path(path))[0].getPath().toString() + String.format("_%010d_chunkfiles/%010d", MID_FILE_SIZE / NON_DEFAULT_BLOCK_SIZE, i));
      }
    }catch(Exception e){
      throw new Exception(String.format("Failed to create File %s",path),e);
    }
  }

  private static void touchFile(String path, long totalFileSize, boolean preserveBlockSize,
                                Options.ChecksumOpt checksumOpt) throws Exception {
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

      FileStatus fileStatus = fs.getFileStatus(qualifiedPath);
      System.out.println(fileStatus.getBlockSize());
      System.out.println(fileStatus.getReplication());
    }
    finally {
      IOUtils.cleanup(null, outputStream);
    }
  }


  @AfterClass
  public static void tearDown() {
    cluster.shutdown();
  }

  @Test
  public void testGetSplitsByChunk() throws Exception {
    try{
      //TODO: DynamicInputChunk has static variable, not supported for a new stat for chunk Test.
      prepareData();
      DistCpOptions options = getOptions();
      options.setMaxMaps(10);
      options.setByChunk(true);
      Configuration configuration = new Configuration();
      configuration.set("mapred.map.tasks",
              String.valueOf(options.getMaxMaps()));
      configuration.setBoolean(DistCpConstants.CONF_LABEL_COPY_BY_CHUNK, true);
      CopyListing.getCopyListing(configuration, CREDENTIALS, options).buildListing(
              new Path(cluster.getFileSystem().getUri().toString()
                      + "/tmp/testDynInputFormatByChunk/fileList.seq"), options);

      JobContext jobContext = new JobContextImpl(configuration, new JobID());
      DynamicInputFormat<Text, CopyListingChunkFileStatus> inputFormat =
              new DynamicInputFormat<Text, CopyListingChunkFileStatus>();
      List<InputSplit> splits = inputFormat.getSplits(jobContext);

      int nFiles = 0;
      int taskId = 0;

      for (InputSplit split : splits) {
        RecordReader<Text, CopyListingChunkFileStatus> recordReader =
                inputFormat.createRecordReader(split, null);
        StubContext stubContext = new StubContext(jobContext.getConfiguration(),
                recordReader, taskId);
        final TaskAttemptContext taskAttemptContext
                = stubContext.getContext();

        recordReader.initialize(splits.get(0), taskAttemptContext);
        float previousProgressValue = 0f;
        while (recordReader.nextKeyValue()) {
          // TODO: for chunkFile
          CopyListingChunkFileStatus fileStatus = recordReader.getCurrentValue();
          String source = recordReader.getCurrentKey().toString();
          source = cluster.getFileSystem().getUri().toString() + "/tmp" + source;
          System.out.println(source);
          Assert.assertTrue(expectedChunkFilePaths.contains(source));
          final float progress = recordReader.getProgress();
          Assert.assertTrue(progress >= previousProgressValue);
          Assert.assertTrue(progress >= 0.0f);
          Assert.assertTrue(progress <= 1.0f);
          if(progress==1.0f){
            //TODO: FOR debug
            System.out.print("test");
          }
          previousProgressValue = progress;
          ++nFiles;
        }
        Assert.assertTrue(recordReader.getProgress() == 1.0f);
        IOUtils.closeStream(recordReader);

        ++taskId;
      }

      Assert.assertEquals(expectedChunkFilePaths.size(), nFiles);
    } finally {
      TestDistCpUtils.delete(cluster.getFileSystem(), "/tmp");
      DynamicInputChunk.resetForTesting();
    }
  }

}
