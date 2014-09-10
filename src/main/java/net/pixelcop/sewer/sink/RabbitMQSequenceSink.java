package net.pixelcop.sewer.sink;

import java.io.IOException;

import net.pixelcop.sewer.DrainSink;
import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.node.Node;
import net.pixelcop.sewer.util.HdfsUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import RabbitMQ stuff needed to create and send information
import net.pixelcop.sewer.SendRabbitMQTopic;
//end of rabbitmq imports

/**
 * A sink which writes to a {@link SequenceFile}, on any filesystem supported by hadoop.
 *
 * @author chetan
 *
 */
@DrainSink
public class RabbitMQSequenceSink extends SequenceFileSink {

  private static final Logger LOG = LoggerFactory.getLogger(RabbitMQSequenceSink.class);

  private static final VLongWritable ONE = new VLongWritable(1L);

   //RabbitMQ
  SendRabbitMQTopic sendRabbit;
  //end

  /**
   * Configured path to write to
   */
  protected String configPath;

  /**
   * Reference to {@link Path} object
   */
  protected Path dstPath;

  protected Writer writer;

  public RabbitMQSequenceSink(String[] args) {
    if (LOG.isInfoEnabled()) {
      LOG.info(":::START RabbitMQSequenceSink Constructor");
    }
    super(args);
    //RabbitMQ
    sendRabbit = new SendRabbitMQTopic();
    //end
    if (LOG.isInfoEnabled()) {
      LOG.info(":::END RabbitMQSequenceSink Constructor");
    }
  }

  @Override
  public void close() throws IOException {
    if (LOG.isInfoEnabled()) {
      LOG.info(":::START RabbitMQSequenceSink close()");
    }
    super.close();
    //RabbitMQ
    sendRabbit.close();
    //end
    if (LOG.isInfoEnabled()) {
      LOG.info(":::END RabbitMQSequenceSink close()");
    }
  }

  @Override
  public void open() throws IOException {
    if (LOG.isInfoEnabled()) {
      LOG.info(":::START RabbitMQSequenceSink open()");
    }
    super.open();
    //RabbitMQ
    sendRabbit.open();
    //end
    if (LOG.isInfoEnabled()) {
      LOG.info(":::END RabbitMQSequenceSink open()");
    }
  }

  @Override
  public void append(Event event) throws IOException {
    if (LOG.isInfoEnabled()) {
      LOG.info(":::START RabbitMQSequenceSink append()");
    }
    super.append(event);
    //RabbitMQ
    sendRabbit.sendMessage("This is a test message of routingKey of: l.ghostery.com");
    //end

    if (LOG.isInfoEnabled()) {
      LOG.info(":::END RabbitMQSequenceSink append()");
    }
  }

}
