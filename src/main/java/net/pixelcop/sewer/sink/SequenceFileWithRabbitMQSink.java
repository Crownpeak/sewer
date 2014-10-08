package net.pixelcop.sewer.sink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

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
  private BlockingQueue<RabbitMessageBatch> batches = new LinkedBlockingQueue<RabbitMessageBatch>(); 
//  private List<RabbitMessageBatch> batches = new LinkedList<RabbitMessageBatch>(); 

  public SequenceFileWithRabbitMQSink(String[] args) {
	super(args);
	atomicCount.incrementAndGet();
  }

  @Override
  public void close() throws IOException {
	  //sends each batch (different hosts) to SendRabbit
	  int size = batches.size();
//	  LOG.info();
	  if( !TransactionManager.sendRabbit.isAlive() )
		  TransactionManager.restartRabbit();
	  for(RabbitMessageBatch batch : batches ) {
		  LOG.info("RABBITMQ: Sending batch of host to Rabbit: "+batch.getHost() + " , SIZE: "+batch.getCount() + " , BATCHES.SIZE(): "+batches.size());
		  TransactionManager.sendRabbit.putBatch(batch);
	  }
	  super.close();
  }
  
  @Override
  public void open() throws IOException {
	  super.open();
  }
  
  private AtomicInteger atomicCount = new AtomicInteger();
  
  @Override
  public void append(Event event) throws IOException {
    super.append(event);
    //adding to Rabbit message,adds to the RabbitMessageBatch object that has the same host, if no matches creates one with that host and adds.
    boolean done = false;
    for(RabbitMessageBatch rmb : batches) {
//    	done = rmb.checkHostAndAddMessage(event.toString(), ((AccessLogWritable)event).getHost());
    	done = rmb.checkHostAndAddMessage(""+atomicCount.get()+" , "+((AccessLogWritable)event).getHost(), ((AccessLogWritable)event).getHost());
    	if(done) {
    	    LOG.info("RABBITMQ: Append for, Count: "+atomicCount.incrementAndGet() + " , Host: "+((AccessLogWritable)event).getHost());
    		break;
    	}
    }
    if(!done) {
    	RabbitMessageBatch newBatch = TransactionManager.sendRabbit.new RabbitMessageBatch(((AccessLogWritable)event).getHost());
//    	done = newBatch.checkHostAndAddMessage(event.toString(), ((AccessLogWritable)event).getHost());
    	done = newBatch.checkHostAndAddMessage(""+atomicCount.get()+" , "+((AccessLogWritable)event).getHost(), ((AccessLogWritable)event).getHost());
        LOG.info("RABBITMQ: New Batch, Append for, Count: "+atomicCount.incrementAndGet() + " , Host: "+((AccessLogWritable)event).getHost());
    	batches.add(newBatch);
    }
    if( !done )
    	LOG.error("RABBITMQ: ERROR: Message not added to batch!\t"+atomicCount.get());
  }

}
