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
 * A sink which writes to a {@link SequenceFile}, on any filesystem supported by hadoop, and also writes to rabbitmq.
 *
 * @author richard craparotta
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
    super(args);
    //RabbitMQ
    sendRabbit = new SendRabbitMQTopic();
    //end
  }

  @Override
  public void close() throws IOException {
    super.close();
    //RabbitMQ
    sendRabbit.close();
    //end
  }

  @Override
  public void open() throws IOException {
    super.open();
    //RabbitMQ
    sendRabbit.open();
    //end
  }

  @Override
  public void append(Event event) throws IOException {
    super.append(event);
    //RabbitMQ
    String eventString = event.toString();

    
    sendRabbit.sendMessage("This is a test message from RabbitMQSequenceSink of routingKey of: l.ghostery.com");
    //end
  }

}
