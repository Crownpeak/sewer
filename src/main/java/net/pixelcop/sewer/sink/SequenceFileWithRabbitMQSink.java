package net.pixelcop.sewer.sink;

import java.io.IOException;
import net.pixelcop.sewer.DrainSink;
import net.pixelcop.sewer.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.pixelcop.sewer.sink.durable.TransactionManager;

import com.evidon.nerf.AccessLogWritable;

/**
 * @author richard craparotta
 */
@DrainSink
public class SequenceFileWithRabbitMQSink extends SequenceFileSink {

  private static final Logger LOG = LoggerFactory.getLogger(SequenceFileWithRabbitMQSink.class);

  public SequenceFileWithRabbitMQSink(String[] args) {
	super(args);
  }

  @Override
  public void close() throws IOException {
	  super.close();
  }

  @Override
  public void open() throws IOException {
    super.open();
    TransactionManager.sendRabbit.open();
  }
  
  @Override
  public void append(Event event) throws IOException {
    super.append(event);
    TransactionManager.rabbitMessageQueue.push(event.toString() + TransactionManager.rabbitMessageDelimeter + ((AccessLogWritable)event).getHost() );
//   	TransactionManager.sendRabbit.sendMessage(event.toString(),((AccessLogWritable)event).getHost());
  }

}