package org.apache.hadoop.tools;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileStatus;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * TODO : add comments
 * CopyChunkFileStatus is a specialized subclass of {@link CopyListingFileStatus} for
 * support copy by chunk to distcp.  This class does not
 * override {@link FileStatus#compareTo}, because the additional data members
 * are not relevant to sort order.
 */
@InterfaceAudience.Private
public class CopyListingChunkFileStatus extends CopyListingFileStatus {
  private long offset;
  private long chunkLength;
  private int chunkIndex;
  private int totalChunkNum;

  public CopyListingChunkFileStatus() {
  }

  public CopyListingChunkFileStatus(CopyListingFileStatus status, long offset, long chunkLength, int chunkIndex, int totalChunkNum) throws IOException{
    super(status);
    this.offset = offset;
    this.chunkLength = chunkLength;
    this.chunkIndex = chunkIndex;
    this.totalChunkNum = totalChunkNum;
  }

  /**
   * get the offset for this chunk
   * @return long offset
   */
  public long getOffset() {
    return offset;
  }

  /**
   * set the offset of the super File for the chunk to read
   * @param offset - The offset for the chunk to read
   */
  public void setOffset(long offset) {
    this.offset = offset;
  }

  /**
   * get the length of this chunk
   * @return long chunkLength
   */
  public long getChunkLength() {
    return chunkLength;
  }

  /**
   * set the chunkLength for this chunk
   * @param chunkLength - The length of this chunk
   */
  public void setChunkLength(long chunkLength) {
    this.chunkLength = chunkLength;
  }

  /**
   * get the chunkIndex of this chunk, it will be used to stitch chunks
   * @return int chunkIndex
   */
  public int getChunkIndex() {
    return chunkIndex;
  }

  /**
   * set the index of this chunk
   * @param chunkIndex - the int index of this chunk in the ordered list of the original file
   */
  public void setChunkIndex(int chunkIndex) {
    this.chunkIndex = chunkIndex;
  }

  public int getTotalChunkNum() {
    return totalChunkNum;
  }

  @Override
  public long getCopyLength(){
    return chunkLength;
  }

  /**
   * set the TotalChunkNum of this chunk
   * @param totalChunkNum
   */
  public void setTotalChunkNum(int totalChunkNum) {
    this.totalChunkNum = totalChunkNum;
  }

  //TODO : test sequenceFile
  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeLong(this.getOffset());
    out.writeLong(this.getChunkLength());
    out.writeInt(this.getChunkIndex());
    out.writeInt(this.getTotalChunkNum());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.setOffset(in.readLong());
    this.setChunkLength(in.readLong());
    this.setChunkIndex(in.readInt());
    this.setTotalChunkNum(in.readInt());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof CopyListingChunkFileStatus)) return false;
    if (!super.equals(o)) return false;

    CopyListingChunkFileStatus that = (CopyListingChunkFileStatus) o;

    if (chunkIndex != that.chunkIndex) return false;
    if (chunkLength != that.chunkLength) return false;
    if (offset != that.offset) return false;
    if (totalChunkNum != that.totalChunkNum) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (int) (offset ^ (offset >>> 32));
    result = 31 * result + (int) (chunkLength ^ (chunkLength >>> 32));
    result = 31 * result + chunkIndex;
    result = 31 * result + totalChunkNum;
    return result;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(super.toString());
    sb.append("CopyListingChunkFileStatus{" +
            "offset=" + offset +
            ", chunkLength=" + chunkLength +
            ", chunkIndex=" + chunkIndex +
            ", totalChunkNum=" + totalChunkNum +
            '}');
    return sb.toString();
  }
}
