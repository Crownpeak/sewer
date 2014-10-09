package net.pixelcop.sewer.sink;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import net.pixelcop.sewer.DrainSink;
import net.pixelcop.sewer.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.pixelcop.sewer.SendRabbitMQ;
import net.pixelcop.sewer.sink.durable.TransactionManager;

import com.evidon.nerf.AccessLogWritable;

/**
 * @author richard craparotta
 */
@DrainSink
public class SequenceFileWithRabbitMQSink extends SequenceFileSink {

//  private BlockingQueue<RabbitMessageBatch> batches = new LinkedBlockingQueue<RabbitMessageBatch>(); 

  private static final Logger LOG = LoggerFactory.getLogger(SequenceFileWithRabbitMQSink.class);
  private BlockingQueue<String> batch = new LinkedBlockingQueue<String>();


  public SequenceFileWithRabbitMQSink(String[] args) {
	super(args);
  }

  @Override
  public void close() throws IOException {
	  if( !TransactionManager.sendRabbit.isAlive() ) {
//		  TransactionManager.sendRabbit.close();
		  TransactionManager.restartRabbit();
	  }
	  LOG.info("RABBITMQ: Sending batch of Size: "+batch.size());
//	  try {
//		TransactionManager.batchQueue.put(batch);
//	} catch (InterruptedException e) {
//		LOG.error("RABBITMQ: Error, batch not added to Queue! Batch Lost!");
//		e.printStackTrace();
//	}
	  TransactionManager.sendRabbit.putBatch(batch);
	  super.close();
  }
  
  @Override
  public void open() throws IOException {
	  super.open();
  }
  
//  private AtomicInteger atomicCount = new AtomicInteger();
  
  @Override
  public void append(Event event) throws IOException {
    super.append(event);
    //adding to Rabbit message,adds to the RabbitMessageBatch object that has the same host, if no matches creates one with that host and adds.
    if( LOG.isDebugEnabled() ) {
		LOG.info("RABBITMQ: Appending Message: "+event.toString());
	}
    try {
    	if( batch.size() == 0) {
    		batch.put(event.toString());
    	}
    	else {
    		batch.put("\n"+event.toString());
    	}
	} catch (InterruptedException e) {
    	LOG.error("RABBITMQ: ERROR: Message not added to batch!");
		e.printStackTrace();
	}
//    boolean done = false;
//    for(RabbitMessageBatch rmb : batches) {
//    	done = rmb.checkHostAndAddMessage("\n"+event.toString(), ((AccessLogWritable)event).getHost());
////    	done = rmb.checkHostAndAddMessage("\n"+atomicCount.get()+" , "+((AccessLogWritable)event).getHost(), ((AccessLogWritable)event).getHost());
//    	if(done) {
//    		if( LOG.isDebugEnabled() ) {
//    			LOG.info("RABBITMQ: Append to Host: "+((AccessLogWritable)event).getHost());
//    		}
////    		atomicCount.incrementAndGet();
//    		break;
//    	}
//    }
//    if(!done) {
//    	RabbitMessageBatch newBatch = TransactionManager.sendRabbit.new RabbitMessageBatch(((AccessLogWritable)event).getHost());
//    	done = newBatch.checkHostAndAddMessage(event.toString(), ((AccessLogWritable)event).getHost());
////    	done = newBatch.checkHostAndAddMessage(atomicCount.incrementAndGet()+" , "+((AccessLogWritable)event).getHost(), ((AccessLogWritable)event).getHost());
//		if( LOG.isDebugEnabled() ) {
//			LOG.info("RABBITMQ: New Batch, Append to Host: "+((AccessLogWritable)event).getHost());
//		}
//    	batches.add(newBatch);
//    }
//    if( !done ) {
//    	LOG.error("RABBITMQ: ERROR: Message not added to batch!");
//    }
  }

}
