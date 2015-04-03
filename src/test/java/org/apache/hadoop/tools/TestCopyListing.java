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

import static org.mockito.Mockito.*;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.tools.util.DistCpUtils;
import org.apache.hadoop.tools.util.TestDistCpUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.AfterClass;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.ArrayList;

public class TestCopyListing extends SimpleCopyListing {
  private static final Log LOG = LogFactory.getLog(TestCopyListing.class);

  private static final Credentials CREDENTIALS = new Credentials();

  private static final Configuration config = new Configuration();
  private static MiniDFSCluster cluster;

  @BeforeClass
  public static void create() throws IOException {
    cluster = new MiniDFSCluster.Builder(config).numDataNodes(1).format(true)
                                                .build();
  }

  @AfterClass
  public static void destroy() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
  
  public TestCopyListing() {
    super(config, CREDENTIALS);
  }

  protected TestCopyListing(Configuration configuration) {
    super(configuration, CREDENTIALS);
  }

  @Override
  protected long getBytesToCopy() {
    return 0;
  }

  @Override
  @VisibleForTesting
  public long getNumberOfPaths() {
    return 0;
  }

  @Test(timeout=10000)
  public void testSkipCopy() throws Exception {
    SimpleCopyListing listing = new SimpleCopyListing(getConf(), CREDENTIALS) {
      @Override
      protected boolean shouldCopy(Path path, DistCpOptions options) {
        return !path.getName().equals(FileOutputCommitter.SUCCEEDED_FILE_NAME);
      }
    };
    FileSystem fs = FileSystem.get(getConf());
    List<Path> srcPaths = new ArrayList<Path>();
    srcPaths.add(new Path("/tmp/in4/1"));
    srcPaths.add(new Path("/tmp/in4/2"));
    Path target = new Path("/tmp/out4/1");
    TestDistCpUtils.createFile(fs, "/tmp/in4/1/_SUCCESS");
    TestDistCpUtils.createFile(fs, "/tmp/in4/1/file");
    TestDistCpUtils.createFile(fs, "/tmp/in4/2");
    fs.mkdirs(target);
    DistCpOptions options = new DistCpOptions(srcPaths, target);
    options.setByChunk(true);
    Path listingFile = new Path("/tmp/list4");
    listing.buildListing(listingFile, options);
    Assert.assertEquals(listing.getNumberOfPaths(), 3);
    SequenceFile.Reader reader = new SequenceFile.Reader(getConf(),
        SequenceFile.Reader.file(listingFile));
    if(options.getByChunk()){
      CopyListingChunkFileStatus fileStatus = new CopyListingChunkFileStatus();
      Text relativePath = new Text();
      Assert.assertTrue(reader.next(relativePath, fileStatus));
      //folder as no chunk
      Assert.assertEquals(relativePath.toString(), "/1");
      Assert.assertTrue(reader.next(relativePath, fileStatus));
      Assert.assertEquals(relativePath.toString(), "/1/file_0000000001_chunkfiles/0000000000");
      Assert.assertTrue(reader.next(relativePath, fileStatus));
      Assert.assertEquals(relativePath.toString(), "/2_0000000001_chunkfiles/0000000000");
      Assert.assertFalse(reader.next(relativePath, fileStatus));
    }else {
      CopyListingFileStatus fileStatus = new CopyListingFileStatus();
      Text relativePath = new Text();
      Assert.assertTrue(reader.next(relativePath, fileStatus));
      Assert.assertEquals(relativePath.toString(), "/1");
      Assert.assertTrue(reader.next(relativePath, fileStatus));
      Assert.assertEquals(relativePath.toString(), "/1/file");
      Assert.assertTrue(reader.next(relativePath, fileStatus));
      Assert.assertEquals(relativePath.toString(), "/2");
      Assert.assertFalse(reader.next(relativePath, fileStatus));
    }
  }

  @Test(timeout=10000)
  public void testChunkSplit() throws Exception {
    FileSystem fs = null;
    try {
      fs = FileSystem.get(getConf());
      List<Path> srcPaths = new ArrayList<Path> ();
      srcPaths.add(new Path("/tmp/in/"));
      Path target = new Path("/tmp/out/");

      // for fileSize  = blocksize*N+M, M!=0,  9
      long fileSize1 = 274*1024;
      long blockSize1 = 32*1024;
      short replic1 = 2;
      Path file1 = new Path("/tmp/in/1");
      TestDistCpUtils.createFile(fs,file1,fileSize1,replic1,blockSize1);
      Assert.assertEquals(fs.getFileStatus(file1).getLen(), fileSize1);
      Assert.assertEquals(fs.getFileStatus(file1).getBlockSize(),blockSize1);
      Assert.assertEquals(fs.getFileStatus(file1).getReplication(), replic1);

      // for fileSize < blocksize  1
      long fileSize2 = 20*1024;
      long blockSize2 = 32*1024;
      short replic2 = 1;
      Path file2 = new Path("/tmp/in/2");
      TestDistCpUtils.createFile(fs,file2,fileSize2,replic2,blockSize2);
      Assert.assertEquals(fs.getFileStatus(file2).getLen(), fileSize2);
      Assert.assertEquals(fs.getFileStatus(file2).getBlockSize(),blockSize2);
      Assert.assertEquals(fs.getFileStatus(file2).getReplication(),replic2);

      // for empty file  1
      Path fileEmpty = new Path("/tmp/in/fileEmpty");
      TestDistCpUtils.createFile(fs,fileEmpty);
      Assert.assertEquals(fs.getFileStatus(fileEmpty).getLen(), 0);

      // for empty folder 3
      Path folderEmpty = new Path("/tmp/in/emptyFolder/inEmptyFolder/inInEmptyFolder");
      TestDistCpUtils.createDirectory(fs, folderEmpty);

      fs.mkdirs(target);
      SimpleCopyListing listing = new SimpleCopyListing(getConf(), CREDENTIALS);
      DistCpOptions options = OptionsParser.parse(
              new String[]{
                      "-pb",
                      "-bychunk",
                      fs.getFileStatus(new Path("/tmp/in/")).getPath().toString(),
                      fs.getFileStatus(target).getPath().toString()}
      );
      //SourceRootPath is parent /tmp
      options.setTargetPathExists(true);
      Assert.assertEquals(options.shouldPreserve(DistCpOptions.FileAttribute.getAttribute('b')),true);
      Assert.assertEquals(options.getByChunk(),true);
      Path listingFile = new Path("/tmp/list4");
      listing.buildListing(listingFile, options);
      Assert.assertEquals(9+1+1+1+3,listing.getNumberOfPaths());
      SequenceFile.Reader reader = new SequenceFile.Reader(getConf(),
              SequenceFile.Reader.file(listingFile));
      if(options.getByChunk()) {
        CopyListingChunkFileStatus fileStatus = new CopyListingChunkFileStatus();
        Text relativePath = new Text();
        long offset = 0;
        while (reader.next(relativePath, fileStatus)) {
          int chunkNum = fileStatus.getTotalChunkNum();
          int chunkIndex = fileStatus.getChunkIndex();
          if (fileStatus.isDirectory()) {
            Assert.assertEquals(DistCpUtils.getRelativePath(new Path("/tmp/"), fileStatus.getPath()).toString(), relativePath.toString());
          } else {
            //assert relPath fileName
            Assert.assertEquals(DistCpUtils.getChunkRelativePath(new Path("/tmp/"), fileStatus.getPath(), chunkIndex, chunkNum).toString(), relativePath.toString());
            if(fileStatus.getChunkIndex()==0){
              //reset offset = 0
              offset = 0;
            }

            if(fileStatus.getChunkIndex()==fileStatus.getTotalChunkNum()-1){
              //lastchunk
              Assert.assertEquals(fileStatus.getLen()%fileStatus.getBlockSize(),fileStatus.getChunkLength());
              Assert.assertEquals(offset,fileStatus.getOffset());
              Assert.assertEquals(fileStatus.getLen() - fileStatus.getChunkLength(),fileStatus.getOffset());
            }else{
              Assert.assertEquals(fileStatus.getBlockSize()*1,fileStatus.getChunkLength());
              Assert.assertEquals(offset,fileStatus.getOffset());
              offset+=fileStatus.getChunkLength();
            }
          }
        }
      }
    } catch (IOException e){
      throw e;
    } finally{
      TestDistCpUtils.delete(fs, "/tmp");
    }
  }

  @Test(timeout=10000)
  public void testMultipleSrcToFile() {
    FileSystem fs = null;
    try {
      fs = FileSystem.get(getConf());
      List<Path> srcPaths = new ArrayList<Path>();
      srcPaths.add(new Path("/tmp/in/1"));
      srcPaths.add(new Path("/tmp/in/2"));
      Path target = new Path("/tmp/out/1");
      TestDistCpUtils.createFile(fs, "/tmp/in/1");
      TestDistCpUtils.createFile(fs, "/tmp/in/2");
      fs.mkdirs(target);
      DistCpOptions options = new DistCpOptions(srcPaths, target);
      validatePaths(options);
      TestDistCpUtils.delete(fs, "/tmp");
      //No errors

      target = new Path("/tmp/out/1");
      fs.create(target).close();
      options = new DistCpOptions(srcPaths, target);
      try {
        validatePaths(options);
        Assert.fail("Invalid inputs accepted");
      } catch (InvalidInputException ignore) { }
      TestDistCpUtils.delete(fs, "/tmp");

      srcPaths.clear();
      srcPaths.add(new Path("/tmp/in/1"));
      fs.mkdirs(new Path("/tmp/in/1"));
      target = new Path("/tmp/out/1");
      fs.create(target).close();
      options = new DistCpOptions(srcPaths, target);
      try {
        validatePaths(options);
        Assert.fail("Invalid inputs accepted");
      } catch (InvalidInputException ignore) { }
      TestDistCpUtils.delete(fs, "/tmp");
    } catch (IOException e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("Test input validation failed");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp");
    }
  }

  @Test(timeout=10000)
  public void testDuplicates() {
    FileSystem fs = null;
    try {
      fs = FileSystem.get(getConf());
      List<Path> srcPaths = new ArrayList<Path>();
      srcPaths.add(new Path("/tmp/in/*/*"));
      TestDistCpUtils.createFile(fs, "/tmp/in/src1/1.txt");
      TestDistCpUtils.createFile(fs, "/tmp/in/src2/1.txt");
      Path target = new Path("/tmp/out");
      Path listingFile = new Path("/tmp/list");
      DistCpOptions options = new DistCpOptions(srcPaths, target);
      CopyListing listing = CopyListing.getCopyListing(getConf(), CREDENTIALS, options);
      try {
        listing.buildListing(listingFile, options);
        Assert.fail("Duplicates not detected");
      } catch (DuplicateFileException ignore) {
      }
    } catch (IOException e) {
      LOG.error("Exception encountered in test", e);
      Assert.fail("Test failed " + e.getMessage());
    } finally {
      TestDistCpUtils.delete(fs, "/tmp");
    }
  }

  @Test(timeout=10000)
  public void testBuildListing() {
    FileSystem fs = null;
    try {
      fs = FileSystem.get(getConf());
      List<Path> srcPaths = new ArrayList<Path>();
      Path p1 = new Path("/tmp/in/1");
      Path p2 = new Path("/tmp/in/2");
      Path p3 = new Path("/tmp/in2/2");
      Path target = new Path("/tmp/out/1");
      srcPaths.add(p1.getParent());
      srcPaths.add(p3.getParent());
      TestDistCpUtils.createFile(fs, "/tmp/in/1");
      TestDistCpUtils.createFile(fs, "/tmp/in/2");
      TestDistCpUtils.createFile(fs, "/tmp/in2/2");
      fs.mkdirs(target);
      OutputStream out = fs.create(p1);
      out.write("ABC".getBytes());
      out.close();

      out = fs.create(p2);
      out.write("DEF".getBytes());
      out.close();

      out = fs.create(p3);
      out.write("GHIJ".getBytes());
      out.close();

      Path listingFile = new Path("/tmp/file");

      DistCpOptions options = new DistCpOptions(srcPaths, target);
      options.setSyncFolder(true);
      CopyListing listing = new SimpleCopyListing(getConf(), CREDENTIALS);
      try {
        listing.buildListing(listingFile, options);
        Assert.fail("Duplicates not detected");
      } catch (DuplicateFileException ignore) {
      }
      Assert.assertEquals(listing.getBytesToCopy(), 10);
      Assert.assertEquals(listing.getNumberOfPaths(), 3);
      TestDistCpUtils.delete(fs, "/tmp");

      try {
        listing.buildListing(listingFile, options);
        Assert.fail("Invalid input not detected");
      } catch (InvalidInputException ignore) {
      }
      TestDistCpUtils.delete(fs, "/tmp");
    } catch (IOException e) {
      LOG.error("Exception encountered ", e);
      Assert.fail("Test build listing failed");
    } finally {
      TestDistCpUtils.delete(fs, "/tmp");
    }
  }

  @Test(timeout=10000)
  public void testBuildListingForSingleFile() {
    FileSystem fs = null;
    String testRootString = "/singleFileListing";
    Path testRoot = new Path(testRootString);
    SequenceFile.Reader reader = null;
    try {
      fs = FileSystem.get(getConf());
      if (fs.exists(testRoot))
        TestDistCpUtils.delete(fs, testRootString);

      Path sourceFile = new Path(testRoot, "/source/foo/bar/source.txt");
      Path decoyFile  = new Path(testRoot, "/target/moo/source.txt");
      Path targetFile = new Path(testRoot, "/target/moo/target.txt");

      TestDistCpUtils.createFile(fs, sourceFile.toString());
      TestDistCpUtils.createFile(fs, decoyFile.toString());
      TestDistCpUtils.createFile(fs, targetFile.toString());

      List<Path> srcPaths = new ArrayList<Path>();
      srcPaths.add(sourceFile);

      DistCpOptions options = new DistCpOptions(srcPaths, targetFile);
      CopyListing listing = new SimpleCopyListing(getConf(), CREDENTIALS);

      final Path listFile = new Path(testRoot, "/tmp/fileList.seq");
      listing.buildListing(listFile, options);

      reader = new SequenceFile.Reader(getConf(), SequenceFile.Reader.file(listFile));

      CopyListingFileStatus fileStatus = new CopyListingFileStatus();
      Text relativePath = new Text();
      Assert.assertTrue(reader.next(relativePath, fileStatus));
      Assert.assertTrue(relativePath.toString().equals(""));
    }
    catch (Exception e) {
      Assert.fail("Unexpected exception encountered.");
      LOG.error("Unexpected exception: ", e);
    }
    finally {
      TestDistCpUtils.delete(fs, testRootString);
      IOUtils.closeStream(reader);
    }
  }
  
  @Test
  public void testFailOnCloseError() throws IOException {
    File inFile = File.createTempFile("TestCopyListingIn", null);
    inFile.deleteOnExit();
    File outFile = File.createTempFile("TestCopyListingOut", null);
    outFile.deleteOnExit();
    List<Path> srcs = new ArrayList<Path>();
    srcs.add(new Path(inFile.toURI()));
    
    Exception expectedEx = new IOException("boom");
    SequenceFile.Writer writer = mock(SequenceFile.Writer.class);
    doThrow(expectedEx).when(writer).close();
    
    SimpleCopyListing listing = new SimpleCopyListing(getConf(), CREDENTIALS);
    DistCpOptions options = new DistCpOptions(srcs, new Path(outFile.toURI()));
    Exception actualEx = null;
    try {
      listing.doBuildListing(writer, options);
    } catch (Exception e) {
      actualEx = e;
    }
    Assert.assertNotNull("close writer didn't fail", actualEx);
    Assert.assertEquals(expectedEx, actualEx);
  }
}
