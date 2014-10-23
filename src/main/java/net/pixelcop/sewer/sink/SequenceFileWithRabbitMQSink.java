package net.pixelcop.sewer.sink;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import net.pixelcop.sewer.DrainSink;
import net.pixelcop.sewer.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.pixelcop.sewer.sink.durable.TransactionManager;

/**
 * @author richard craparotta
 */
@DrainSink
public class SequenceFileWithRabbitMQSink extends SequenceFileSink {

  private static final Logger LOG = LoggerFactory.getLogger(SequenceFileWithRabbitMQSink.class);
  private BlockingQueue<String> batch = new LinkedBlockingQueue<String>();

  public SequenceFileWithRabbitMQSink(String[] args) {
	super(args);
  }

  @Override
  public void close() throws IOException {
	  if( !TransactionManager.sendRabbit.isAlive() )
		  TransactionManager.restartRabbit();
	  LOG.info("RABBITMQ: Sending batch of Size: "+batch.size());
	  TransactionManager.sendRabbit.putBatch(batch);
	  super.close();
  }
  
  @Override
  public void open() throws IOException {
	  if( TransactionManager.rabbitEnabled ) { 
		  super.open();
	  }
	  else {
		  LOG.error("RABBITMQ: rmq.enabled = false. Fix in config.properties and restart.");
		  throw new IOException();
	  }
  }
    
  @Override
  public void append(Event event) throws IOException {
    super.append(event);
    if( LOG.isDebugEnabled() )
		LOG.info("RABBITMQ: Appending Message: "+event.toString());
    try {
    	if( batch.size() == 0)
    		batch.put(event.toString());
       	else
    		batch.put("\n"+event.toString());
	} catch (InterruptedException e) {
    	LOG.error("RABBITMQ: ERROR: Message not added to batch!");
		e.printStackTrace();
	}
  }

}
