package net.pixelcop.sewer.sink;

import java.io.IOException;

import net.pixelcop.sewer.ByteArrayEvent;
import net.pixelcop.sewer.Event;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.NativeCodeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple wrapper around {@link FSDataOutputStream} with compression
 *
 * @author chetan
 *
 */
public class SequenceFileSink extends BucketedSink {

  private static final Logger LOG = LoggerFactory.getLogger(SequenceFileSink.class);

  private static final NullWritable NULL = NullWritable.get();

  /**
   * Configured DFS path to write to
   */
  private String configPath;

  /**
   * Reference to DFS Path object
   */
  private Path dstPath;

  private Writer writer;

  public SequenceFileSink(String[] args) {
    this.configPath = args[0];
  }

  @Override
  public void close() throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Closing SequenceFileSink " + configPath);
    }

    if (writer != null) {
      writer.close();

    } else if (getStatus() == OPENING) {
      // closed down while in the process of opening.
      // quick open/close, dead client, etc
      while (getStatus() != FLOWING) {
      }
      writer.close();
    }
    nextBucket = null;
    setStatus(CLOSED);
  }

  @Override
  public void open() throws IOException {
    setStatus(OPENING);
    if (nextBucket == null) {
      generateNextBucket();
    }
    createWriter();
    setStatus(FLOWING);
  }

  private void createWriter() throws IOException {

    Configuration conf = new Configuration();
    conf.setInt("io.file.buffer.size", 16384*4); // temp workaround until we fix Config

    CompressionCodec codec = createCodec();

    dstPath = new Path(nextBucket + ".seq" + codec.getDefaultExtension());
    FileSystem hdfs = dstPath.getFileSystem(conf);

    writer = SequenceFile.createWriter(
        hdfs, conf, dstPath, NullWritable.class, ByteArrayEvent.class, CompressionType.BLOCK, codec);

    if (LOG.isInfoEnabled()) {
      LOG.info("Created " + codec.getClass().getSimpleName() + " compressed HDFS file: " + dstPath.toString());
    }

    nextBucket = null;
  }

  public CompressionCodec createCodec() {

    // TODO handle pluggable compression codec
    CompressionCodec codec;
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      codec = new GzipCodec();
    } else {
      codec = new DeflateCodec();
    }
    return codec;

  }

  @Override
  public String getFileExt() {
    return ".seq" + createCodec().getDefaultExtension();
  }

  @Override
  public String generateNextBucket() {
    nextBucket = BucketPath.escapeString(configPath, null);
    return nextBucket;
  }

  @Override
  public void append(Event event) throws IOException {
    writer.append(NULL, event);
  }

}
