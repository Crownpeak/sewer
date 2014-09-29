package net.pixelcop.sewer.sink;

import java.io.IOException;
import java.util.ArrayList;
import net.pixelcop.sewer.DrainSink;
import net.pixelcop.sewer.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.pixelcop.sewer.SendRabbitMQTopic;
import net.pixelcop.sewer.sink.durable.DeferWithRabbitMQSink;

import com.evidon.nerf.AccessLogWritable;

/**
 * @author richard craparotta
 */
@DrainSink
public class SequenceFileWithRabbitMQSink extends SequenceFileSink {

  private static final Logger LOG = LoggerFactory.getLogger(SequenceFileWithRabbitMQSink.class);

  private DeferWithRabbitMQSink defer;
//  private SendRabbitMQTopic sendRabbit;

  public SequenceFileWithRabbitMQSink(String[] args) {
	super(args);
//    sendRabbit = new SendRabbitMQTopic();
  }

  @Override
  public void close() throws IOException {
//	  defer.getSendRabbit().close();
	  super.close();
  }

  @Override
  public void open() throws IOException {
    super.open();
    defer.getSendRabbit().open();
  }
  
  @Override
  public void append(Event event) throws IOException {
    super.append(event);
    defer.getSendRabbit() .sendMessage(event.toString(),((AccessLogWritable)event).getHost());
 	
  }
  
  public void setSendRabbit(DeferWithRabbitMQSink defer) {
	  this.defer = defer;
  }

}