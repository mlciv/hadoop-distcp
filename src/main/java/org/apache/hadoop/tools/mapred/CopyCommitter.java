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

package org.apache.hadoop.tools.mapred;

import org.apache.commons.io.filefilter.FalseFileFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.tools.*;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;
import org.apache.hadoop.tools.util.DistCpUtils;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * The CopyCommitter class is DistCp's OutputCommitter implementation. It is
 * responsible for handling the completion/cleanup of the DistCp run.
 * Specifically, it does the following:
 *  1. Cleanup of the meta-folder (where DistCp maintains its file-list, etc.)
 *  2. Preservation of user/group/replication-factor on any directories that
 *     have been copied. (Files are taken care of in their map-tasks.)
 *  3. Atomic-move of data from the temporary work-folder to the final path
 *     (if atomic-commit was opted for).
 *  4. Deletion of files from the target that are missing at source (if opted for).
 *  5. Cleanup of any partially copied files, from previous, failed attempts.
 */
public class CopyCommitter extends FileOutputCommitter {
  private static final Log LOG = LogFactory.getLog(CopyCommitter.class);

  private final TaskAttemptContext taskAttemptContext;
  private boolean syncFolder = false;
  private boolean overwrite = false;
  private boolean targetPathExists = true;
  
  /**
   * Create a output committer
   *
   * @param outputPath the job's output path
   * @param context    the task's context
   * @throws IOException - Exception if any
   */
  public CopyCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    this.taskAttemptContext = context;
  }

  /**
   * Stitch chunks into files, crc check, persevere
   * @param job
   */
  public void stitchChunks(JobContext jobContext) throws IOException{
    //0. All chunks has been copied into target position named /originalFileName_5_chunks/chunkIndex
    //1. read file.seq
    //2. for every index=0 to stitch
    //3. write error to metaFolder\errNote
    LOG.info("Chunks copied, begin to stitch chunks");
    long start = System.currentTimeMillis();
    Configuration conf = jobContext.getConfiguration();
    Path errorPath = new Path(conf.get(DistCpConstants.CONF_LABEL_ERROR_NOTE,
            conf.get(DistCpConstants.CONF_LABEL_META_FOLDER)+"/errorNote"));

    FileSystem fs = errorPath.getFileSystem(conf);
    if(fs.exists(errorPath)){
      fs.delete(errorPath,false);
    }
    PrintWriter errorNote = new PrintWriter(fs.create(errorPath));
    int error = 0;
    int totalFile = 0;
    int totalChunks = 0;
    int totalDir = 0;
    final Path listingFilePath = new Path(jobContext.getConfiguration().get(DistCpConstants.CONF_LABEL_LISTING_FILE_PATH));
    try {
      final FileSystem fileSystem = listingFilePath.getFileSystem(jobContext.getConfiguration());

      if (!fileSystem.exists(listingFilePath))
        throw new IllegalArgumentException("Listing file doesn't exist at: "
                + listingFilePath);

      SequenceFile.Reader reader =  new SequenceFile.Reader(jobContext.getConfiguration(),
              SequenceFile.Reader.file(listingFilePath));
      Text srcRelPath = new Text();
      CopyListingChunkFileStatus srcFileStatus = new CopyListingChunkFileStatus();
      Path targetPath = new Path(jobContext.getConfiguration().get(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH));
      FileSystem targetFS = targetPath.getFileSystem(jobContext.getConfiguration());
      while (reader.next(srcRelPath, srcFileStatus)) {
        totalChunks++;
        if(srcFileStatus.isDirectory()){
          //TODO: directories need not to be stitiched, already ok
          totalDir++;
          continue;
        }
        if(srcFileStatus.getChunkIndex()==0)
        {
          //TODO check DistributeFileSystem, atomic
          Path targetWorkPath = new Path(jobContext.getConfiguration().get(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH));
          Path chunkFolder = new Path(targetWorkPath.makeQualified(targetFS.getUri(),
                  targetFS.getWorkingDirectory()) + srcRelPath.toString()).getParent();
          Path target = new Path(targetWorkPath.makeQualified(targetFS.getUri(),
                  targetFS.getWorkingDirectory()) + DistCpUtils.changeChunkToOriginalPath(srcRelPath.toString()));
          int chunkNum = srcFileStatus.getTotalChunkNum();
          Path[] chunks = new Path[chunkNum];
          // make sure to add chunk in order
          boolean complete = true;
          for (int i = 0; i < chunkNum; i++) {
            Path chunkFile = new Path(chunkFolder, String.format("%010d",i));
            //make sure this file is already copied
            if (!targetFS.exists(chunkFile)) {
              //TODO: handler file incomplete
              error++;
              errorNote.println(srcFileStatus.getPath()+"=:="+chunkFile + "not exist");
              LOG.error("ChunkFile:" + chunkFile.toUri().getPath() + "doesn't exist");
              complete = false;
              continue;
            } else {
              chunks[i] = chunkFile;
            }
          }

          if(!complete){
            //TODO: ignore this file, and continue to stitch
            continue;
          }
          LOG.debug("chunks[0]="+chunks[0].toString()+":"+targetFS.getFileChecksum(chunks[0]));

//          // ger rest filechunks, but concat cannot keep the order of the restChunkFiles
            // for concat call verifySrcFiles, which using hashset to verify the duplicates, and connot
            // keep the insertion order of the set.
//          if(chunkNum>1){
//            Path[] restChunkFiles = new Path[chunkNum-1];
//            System.arraycopy(chunks,1,restChunkFiles,0,chunkNum-1);
//            targetFS.concat(chunks[0],restChunkFiles);
//          }
          if(chunkNum>1){
            for(int j=1;j<chunkNum;j++){
              //TODO: concat one by one, since concat not support keep the order the input array
              targetFS.concat(chunks[0],new Path[]{chunks[j]});
            }
          }

          if(validate(chunks[0],targetFS,srcFileStatus,conf)) {
            //compatible for single chunkfile
            totalFile++;
            rename(targetFS, chunks[0], target);
            targetFS.delete(chunkFolder,true);
            LOG.info("stitchChunks succeeded for (" + srcFileStatus.toString() + ") to target(" + chunks[0].toString());
          } else{
            //not error File into notes.
            //TODO: check errorNote
            error++;
            errorNote.println(srcFileStatus.getPath()+"=:= Checksum mismatch");
            LOG.error("stitchChunks failed for (" + srcFileStatus.toString() + ") to target(" + chunks[0].toString());
          }
        }
      }
    }
    catch (IllegalArgumentException exception){
      LOG.error("Couldn't find listing file at: " + listingFilePath, exception);
      throw new IOException("Couldn't find listing-file at: "
              + listingFilePath, exception);
    }
    catch (IOException exception) {
      throw new IOException("stitched chunks failed",exception);
    }finally {
      IOUtils.closeStream(errorNote);
      LOG.info(String.format("stitched report[%d ms]:(totalChunks = %d, totalFile = %d, totalDir = %d, error = %d), errorNote = (%s)",System.currentTimeMillis()-start,totalChunks,totalFile,totalDir,error,errorPath));
      InputStreamReader isr = new InputStreamReader(fs.open(errorPath));
      BufferedReader br = new BufferedReader(isr);
      String line = "";
      try{
          while((line = br.readLine())!=null){
            LOG.info("ErrorNote:"+line);
          }
      }
      catch(IOException ex){
        LOG.error("failed to print ErrorNote");
      }finally {
        br.close();
        IOUtils.closeStream(isr);
      }
    }
  }

  /**
   * TODO: validate more infos about the chunks
   * @param sChunk
   * @param targetFS
   * @param srcFileStatus
   * @return
   * @throws IOException
   */
  private boolean validate (Path sChunk, FileSystem targetFS,
                                   CopyListingChunkFileStatus srcFileStatus,Configuration conf) throws IOException{
    try{
      long targetLen = targetFS.getFileStatus(sChunk).getLen();
      Path sourcePath = srcFileStatus.getPath();
      FileSystem sourceFS = sourcePath.getFileSystem(conf);
      String fileAttibutesStr = conf.get(DistCpConstants.CONF_LABEL_PRESERVE_STATUS,"");
      EnumSet<FileAttribute> fileAttributes = DistCpUtils.unpackAttributes(fileAttibutesStr);
      final FileChecksum sourceChecksum = fileAttributes
              .contains(FileAttribute.CHECKSUMTYPE) ? sourceFS
              .getFileChecksum(sourcePath) : null;

      boolean lengthEqual = DistCpUtils.compareFileLengths(srcFileStatus, sChunk,conf,targetLen);
      boolean checksumEqual;
      if (!conf.getBoolean(DistCpConstants.CONF_LABEL_SKIP_CRC,false)) {
        checksumEqual = DistCpUtils.compareCheckSums(sourceFS, srcFileStatus.getPath(), sourceChecksum,
                targetFS, sChunk);
      }else{
        checksumEqual = true;
      }
      return lengthEqual&&checksumEqual;
    }catch (Exception e){
      throw new IOException("validate stitched Chunks failed", e);
    }
  }

  private static void rename(FileSystem targetFS, Path src, Path target) throws IOException{
    try{
      if(targetFS.exists(target)){
        targetFS.delete(target,true);
      }

      if(!targetFS.rename(src, target)){
        //TODO rename test
        throw new IOException();
      }
    }
    catch(IOException e){
      throw new IOException("Fail to rename chunk tmp File(="+ src.toString()+") to target File(="+ target.toString()+")");
    }
  }

  /** @inheritDoc */
  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    Configuration conf = jobContext.getConfiguration();
    syncFolder = conf.getBoolean(DistCpConstants.CONF_LABEL_SYNC_FOLDERS, false);
    overwrite = conf.getBoolean(DistCpConstants.CONF_LABEL_OVERWRITE, false);
    targetPathExists = conf.getBoolean(DistCpConstants.CONF_LABEL_TARGET_PATH_EXISTS, true);

    //TODO before commit stitchChunks
    if(jobContext.getConfiguration().getBoolean(DistCpConstants.CONF_LABEL_COPY_BY_CHUNK,false)){
      stitchChunks(jobContext);
    }

    super.commitJob(jobContext);
    cleanupTempFiles(jobContext);
    String attributes = conf.get(DistCpConstants.CONF_LABEL_PRESERVE_STATUS);
    final boolean preserveRawXattrs =
        conf.getBoolean(DistCpConstants.CONF_LABEL_PRESERVE_RAWXATTRS, false);
    if ((attributes != null && !attributes.isEmpty()) || preserveRawXattrs) {
      preserveFileAttributesForDirectories(conf);
    }

    try {
      if (conf.getBoolean(DistCpConstants.CONF_LABEL_DELETE_MISSING, false)) {
        deleteMissing(conf);
      } else if (conf.getBoolean(DistCpConstants.CONF_LABEL_ATOMIC_COPY, false)) {
        commitData(conf);
      }
      taskAttemptContext.setStatus("Commit Successful");
    }
    finally {
      cleanup(conf);
    }
  }

  /** @inheritDoc */
  @Override
  public void abortJob(JobContext jobContext,
                       JobStatus.State state) throws IOException {
    try {
      super.abortJob(jobContext, state);
    } finally {
      cleanupTempFiles(jobContext);
      cleanup(jobContext.getConfiguration());
    }
  }

  private void cleanupTempFiles(JobContext context) {
    try {
      Configuration conf = context.getConfiguration();

      Path targetWorkPath = new Path(conf.get(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH));
      FileSystem targetFS = targetWorkPath.getFileSystem(conf);

      String jobId = context.getJobID().toString();
      deleteAttemptTempFiles(targetWorkPath, targetFS, jobId);
      deleteAttemptTempFiles(targetWorkPath.getParent(), targetFS, jobId);
    } catch (Throwable t) {
      LOG.warn("Unable to cleanup temp files", t);
    }
  }

  private void deleteAttemptTempFiles(Path targetWorkPath,
                                      FileSystem targetFS,
                                      String jobId) throws IOException {

    FileStatus[] tempFiles = targetFS.globStatus(
        new Path(targetWorkPath, ".distcp.tmp." + jobId.replaceAll("job","attempt") + "*"));

    if (tempFiles != null && tempFiles.length > 0) {
      for (FileStatus file : tempFiles) {
        LOG.info("Cleaning up " + file.getPath());
        targetFS.delete(file.getPath(), false);
      }
    }
  }

  /**
   * Cleanup meta folder and other temporary files
   *
   * @param conf - Job Configuration
   */
  private void cleanup(Configuration conf) {
    Path metaFolder = new Path(conf.get(DistCpConstants.CONF_LABEL_META_FOLDER));
    try {
      FileSystem fs = metaFolder.getFileSystem(conf);
      LOG.info("Cleaning up temporary work folder: " + metaFolder);
      fs.delete(metaFolder, true);
    } catch (IOException ignore) {
      LOG.error("Exception encountered ", ignore);
    }
  }

  // This method changes the target-directories' file-attributes (owner,
  // user/group permissions, etc.) based on the corresponding source directories.
  private void preserveFileAttributesForDirectories(Configuration conf) throws IOException {
    String attrSymbols = conf.get(DistCpConstants.CONF_LABEL_PRESERVE_STATUS);
    final boolean syncOrOverwrite = syncFolder || overwrite;

    LOG.info("About to preserve attributes: " + attrSymbols);

    EnumSet<FileAttribute> attributes = DistCpUtils.unpackAttributes(attrSymbols);
    final boolean preserveRawXattrs =
        conf.getBoolean(DistCpConstants.CONF_LABEL_PRESERVE_RAWXATTRS, false);

    Path sourceListing = new Path(conf.get(DistCpConstants.CONF_LABEL_LISTING_FILE_PATH));
    FileSystem clusterFS = sourceListing.getFileSystem(conf);
    //TODO: SequenceFile.Reader next support subclass reading.
    SequenceFile.Reader sourceReader = new SequenceFile.Reader(conf,
                                      SequenceFile.Reader.file(sourceListing));
    long totalLen = clusterFS.getFileStatus(sourceListing).getLen();

    Path targetRoot = new Path(conf.get(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH));

    long preservedEntries = 0;
    try {
      CopyListingFileStatus srcFileStatus;
      if(conf.getBoolean(DistCpConstants.CONF_LABEL_COPY_BY_CHUNK,false)){
        srcFileStatus = new CopyListingChunkFileStatus();
      }else {
        srcFileStatus = new CopyListingFileStatus();
      }
      Text srcRelPath = new Text();

      // Iterate over every source path that was copied.
      while (sourceReader.next(srcRelPath, srcFileStatus)) {
        // File-attributes for files are set at the time of copy,
        // in the map-task.
        if (! srcFileStatus.isDirectory()) continue;

        Path targetFile = new Path(targetRoot.toString() + "/" + srcRelPath);
        //
        // Skip the root folder when syncOrOverwrite is true.
        //
        if (targetRoot.equals(targetFile) && syncOrOverwrite) continue;

        FileSystem targetFS = targetFile.getFileSystem(conf);
        DistCpUtils.preserve(targetFS, targetFile, srcFileStatus, attributes,
            preserveRawXattrs);

        taskAttemptContext.progress();
        taskAttemptContext.setStatus("Preserving status on directory entries. [" +
            sourceReader.getPosition() * 100 / totalLen + "%]");
      }
    } finally {
      IOUtils.closeStream(sourceReader);
    }
    LOG.info("Preserved status on " + preservedEntries + " dir entries on target");
  }

  // write sourcePaths into sourceListingFile
  // This method deletes "extra" files from the target, if they're not
  // available at the source.
  private void deleteMissing(Configuration conf) throws IOException {
    LOG.info("-delete option is enabled. About to remove entries from " +
        "target that are missing in source");

    // Sort the source-file listing alphabetically.
    Path sourceListing = new Path(conf.get(DistCpConstants.CONF_LABEL_CHECK_LISTING_FILE_PATH));

    FileSystem clusterFS = sourceListing.getFileSystem(conf);
    Path sortedSourceListing = DistCpUtils.sortListing(clusterFS, conf, sourceListing);

    // Similarly, create the listing of target-files. Sort alphabetically.
    Path targetListing = new Path(sourceListing.getParent(), "targetListing.seq");
    CopyListing target = new GlobbedCopyListing(new Configuration(conf), null);

    List<Path> targets = new ArrayList<Path>(1);
    Path targetFinalPath = new Path(conf.get(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH));
    targets.add(targetFinalPath);
    DistCpOptions options = new DistCpOptions(targets, new Path("/NONE"));
    //
    // Set up options to be the same from the CopyListing.buildListing's perspective,
    // so to collect similar listings as when doing the copy
    //
    options.setOverwrite(overwrite);
    options.setSyncFolder(syncFolder);
    options.setTargetPathExists(targetPathExists);
    //after stitched chunk
    options.setByChunk(false);
    
    target.buildListing(targetListing, options);
    Path sortedTargetListing = DistCpUtils.sortListing(clusterFS, conf, targetListing);
    long totalLen = clusterFS.getFileStatus(sortedTargetListing).getLen();

    SequenceFile.Reader sourceReader = new SequenceFile.Reader(conf,
                                 SequenceFile.Reader.file(sortedSourceListing));
    SequenceFile.Reader targetReader = new SequenceFile.Reader(conf,
                                 SequenceFile.Reader.file(sortedTargetListing));

    // Walk both source and target file listings.
    // Delete all from target that doesn't also exist on source.
    long deletedEntries = 0;
    try {
      CopyListingFileStatus srcFileStatus = new CopyListingFileStatus();
      Text srcRelPath = new Text();
      CopyListingFileStatus trgtFileStatus = new CopyListingFileStatus();
      Text trgtRelPath = new Text();

      FileSystem targetFS = targetFinalPath.getFileSystem(conf);
      boolean srcAvailable = sourceReader.next(srcRelPath, srcFileStatus);
      while (targetReader.next(trgtRelPath, trgtFileStatus)) {
        // Skip sources that don't exist on target.
        while (srcAvailable && trgtRelPath.compareTo(srcRelPath) > 0) {
          srcAvailable = sourceReader.next(srcRelPath, srcFileStatus);
        }

        if (srcAvailable && trgtRelPath.equals(srcRelPath)) continue;

        // Target doesn't exist at source. Delete.
        boolean result = (!targetFS.exists(trgtFileStatus.getPath()) ||
            targetFS.delete(trgtFileStatus.getPath(), true));
        if (result) {
          LOG.info("Deleted " + trgtFileStatus.getPath() + " - Missing at source");
          deletedEntries++;
        } else {
          throw new IOException("Unable to delete " + trgtFileStatus.getPath());
        }
        taskAttemptContext.progress();
        taskAttemptContext.setStatus("Deleting missing files from target. [" +
            targetReader.getPosition() * 100 / totalLen + "%]");
      }
    } finally {
      IOUtils.closeStream(sourceReader);
      IOUtils.closeStream(targetReader);
    }
    LOG.info("Deleted " + deletedEntries + " from target: " + targets.get(0));
  }

  private void commitData(Configuration conf) throws IOException {

    Path workDir = new Path(conf.get(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH));
    Path finalDir = new Path(conf.get(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH));
    FileSystem targetFS = workDir.getFileSystem(conf);

    LOG.info("Atomic commit enabled. Moving " + workDir + " to " + finalDir);
    if (targetFS.exists(finalDir) && targetFS.exists(workDir)) {
      LOG.error("Pre-existing final-path found at: " + finalDir);
      throw new IOException("Target-path can't be committed to because it " +
          "exists at " + finalDir + ". Copied data is in temp-dir: " + workDir + ". ");
    }

    boolean result = targetFS.rename(workDir, finalDir);
    if (!result) {
      LOG.warn("Rename failed. Perhaps data already moved. Verifying...");
      result = targetFS.exists(finalDir) && !targetFS.exists(workDir);
    }
    if (result) {
      LOG.info("Data committed successfully to " + finalDir);
      taskAttemptContext.setStatus("Data committed successfully to " + finalDir);
    } else {
      LOG.error("Unable to commit data to " + finalDir);
      throw new IOException("Atomic commit failed. Temporary data in " + workDir +
        ", Unable to move to " + finalDir);
    }
  }
}
