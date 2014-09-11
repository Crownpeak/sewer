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
import com.evidon.nerf.AccessLogWritable;
//end of rabbitmq imports

/**
 *
 * @author richard craparotta
 *
 */
@DrainSink
public class RabbitMQSink extends BucketedSink {

  private static final Logger LOG = LoggerFactory.getLogger(RabbitMQSink.class);

  private static final VLongWritable ONE = new VLongWritable(1L);

   //RabbitMQ
  SendRabbitMQTopic sendRabbit;
  //end

  public RabbitMQSink(String[] args) {
    if (LOG.isInfoEnabled()) {
      LOG.info(":::START RabbitMQSink Constructor");
    }
    //RabbitMQ
    sendRabbit = new SendRabbitMQTopic();
    //end
    if (LOG.isInfoEnabled()) {
      LOG.info(":::END RabbitMQSink Constructor");
    }
  }

  @Override
  public void close() throws IOException {
    if (LOG.isInfoEnabled()) {
      LOG.info(":::START RabbitMQSink close()");
    }
    //RabbitMQ
    sendRabbit.close();
    //end
    if (LOG.isInfoEnabled()) {
      LOG.info(":::END RabbitMQSink close()");
    }
  }

  @Override
  public void open() throws IOException {
    if (LOG.isInfoEnabled()) {
      LOG.info(":::START RabbitMQSink open()");
    }
    //RabbitMQ
    sendRabbit.open();
    //end
    if (LOG.isInfoEnabled()) {
      LOG.info(":::END RabbitMQSink open()");
    }
  }

  @Override
  public void append(Event event) throws IOException {
    if (LOG.isInfoEnabled()) {
      LOG.info(":::START RabbitMQSink append()");
    }
    //RabbitMQ
    sendRabbit.sendMessage("This is a test message from RabbitMQSink of routingKey of: l.ghostery.com",((AccessLogWritable)event).getHost());
    //end

    if (LOG.isInfoEnabled()) {
      LOG.info(":::END RabbitMQSink append()");
    }
  }

    @Override
  public String generateNextBucket() {
    return "";
  }
  
  @Override
  public String getFileExt() {
    return "";
  }

}
