package net.pixelcop.sewer.sink;

import java.io.IOException;
import net.pixelcop.sewer.DrainSink;
import net.pixelcop.sewer.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.pixelcop.sewer.sink.durable.TransactionManager;

import java.util.ArrayList;
import java.util.LinkedList;
import com.evidon.nerf.AccessLogWritable;

/**
 * @author richard craparotta
 */
@DrainSink
public class SequenceFileWithRabbitMQSink extends SequenceFileSink {

  private static final Logger LOG = LoggerFactory.getLogger(SequenceFileWithRabbitMQSink.class);

  public ArrayList<String> messageQueue = new ArrayList<String>(); 
  
  public SequenceFileWithRabbitMQSink(String[] args) {
	super(args);
  }

  @Override
  public void close() throws IOException {
	  //send messageQueue to SendRabbitMQTopic instance in TransactionManager
	  TransactionManager.sendRabbit.addQueue(messageQueue);
	  //must be before super.close as it destroys this class instance
	  super.close();
  }

  @Override
  public void open() throws IOException {
    super.open();
//    TransactionManager.sendRabbit.open();
  }
  
  @Override
  public void append(Event event) throws IOException {
    super.append(event);
    messageQueue.add( event.toString() + TransactionManager.rabbitMessageDelimeter + ((AccessLogWritable)event).getHost() );
    
//    if( TransactionManager.rabbitMessageSwitch ) {
//		LOG.info("RABBITMQ: ADDING TO QUEUE 1...");
//    	TransactionManager.rabbitMessageQueue1.add(event.toString() + TransactionManager.rabbitMessageDelimeter + ((AccessLogWritable)event).getHost() );
//    }
//    else {
//		LOG.info("RABBITMQ: ADDING TO QUEUE 2...");
//    	TransactionManager.rabbitMessageQueue2.add(event.toString() + TransactionManager.rabbitMessageDelimeter + ((AccessLogWritable)event).getHost() );
//    }
  }

}