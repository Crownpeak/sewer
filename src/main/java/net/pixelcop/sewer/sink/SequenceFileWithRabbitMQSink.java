package net.pixelcop.sewer.sink;

import java.io.IOException;
import java.util.ArrayList;
import net.pixelcop.sewer.DrainSink;
import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.SendRabbitMQTopic.RabbitMessageBatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.pixelcop.sewer.SendRabbitMQTopic;
import net.pixelcop.sewer.sink.durable.TransactionManager;

import com.evidon.nerf.AccessLogWritable;

/**
 * @author richard craparotta
 */
@DrainSink
public class SequenceFileWithRabbitMQSink extends SequenceFileSink {

  private static final Logger LOG = LoggerFactory.getLogger(SequenceFileWithRabbitMQSink.class);
  private ArrayList<RabbitMessageBatch> batches = new ArrayList<RabbitMessageBatch>(); 
  
  public SequenceFileWithRabbitMQSink(String[] args) {
	super(args);
  }

  @Override
  public void close() throws IOException {
	  //sends each batch (different hosts) to SendRabbit
	  for(RabbitMessageBatch batch : batches )
		  TransactionManager.sendRabbit.putBatch(batch);
	  super.close();
  }
  
  @Override
  public void append(Event event) throws IOException {
    super.append(event);
	
    //adding to Rabbit message,adds to the RabbitMessageBatch object that has the same host, if no matches creates one with that host and adds.
    boolean done = false;
    for(RabbitMessageBatch rmb : batches) {
    	done = rmb.checkHostAndAddMessage(event.toString(), ((AccessLogWritable)event).getHost());
    }
    if(!done) {
    	RabbitMessageBatch newBatch = TransactionManager.sendRabbit.new RabbitMessageBatch(((AccessLogWritable)event).getHost());
    	newBatch.checkHostAndAddMessage(event.toString(), ((AccessLogWritable)event).getHost());
    }
    
//	if( !TransactionManager.sendRabbit.isAlive() ) {
//		TransactionManager.restartRabbit();
//	}
//    TransactionManager.sendRabbit.put(event.toString()+TransactionManager.sendRabbit.testDelimeter+((AccessLogWritable)event).getHost());
  }

}
