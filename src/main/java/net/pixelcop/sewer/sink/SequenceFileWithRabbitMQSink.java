package net.pixelcop.sewer.sink;

import java.io.IOException;
import java.util.ArrayList;
import net.pixelcop.sewer.DrainSink;
import net.pixelcop.sewer.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.pixelcop.sewer.SendRabbitMQTopic;
import com.evidon.nerf.AccessLogWritable;

/**
 * @author richard craparotta
 */
@DrainSink
public class SequenceFileWithRabbitMQSink extends SequenceFileSink {

  private static final Logger LOG = LoggerFactory.getLogger(SequenceFileWithRabbitMQSink.class);

  SendRabbitMQTopic sendRabbit;
  ArrayList<MessageBatch> rabbitMessages = new ArrayList<MessageBatch>();

  public SequenceFileWithRabbitMQSink(String[] args) {
	super(args);
    sendRabbit = new SendRabbitMQTopic();
  }

  @Override
  public void close() throws IOException {
    super.close();
    sendRabbit.close();
  }

  @Override
  public void open() throws IOException {
    super.open();
    sendRabbit.open();
  }
  
  @Override
  public void append(Event event) throws IOException {
    super.append(event);
    
	sendRabbit.sendMessage(event.toString(),((AccessLogWritable)event).getHost());

    
//    boolean added = false;
//    for( MessageBatch mb : rabbitMessages ) {
//    	if( mb.isHostMatch( ((AccessLogWritable)event).getHost() ) ) {
//    		mb.addEvent(event.toString());
//    		added = true;
//    		break;
//    	}
//    }
//    if( !added ) {
//    	MessageBatch mb = new MessageBatch( ((AccessLogWritable)event).getHost() );
//    	mb.addEvent(event.toString());
//    	rabbitMessages.add(mb);
//    }
	
  }
  
//  @Override
//  public void sendRabbitMessage() {
//	  for( MessageBatch mb : rabbitMessages ) {
//		  sendRabbit.sendMessage(mb.getAppendedMessage(),mb.getHost());
//	  }
//  }
  
  public class MessageBatch {
	  
	  String host;
	  String eventDelimeter = "\n";
	  ArrayList<String> events = new ArrayList<String>();
	  
	  public MessageBatch(String host) {
		  this.host=host;
	  }
	  
	  public String getHost() {
		  return host;
	  }
	  
	  public boolean isHostMatch(String s) {
		  return host.equals(s);
	  }
	  
	  public void addEvent(String event) {
		  events.add(event);
	  }
	  
	  public String getAppendedMessage() {
		  String retVal = "";
		  for( int i = 0; i < events.size(); i++) {
			  retVal += events.get(i);
			  if( i+1 < events.size() ) {
				  retVal += eventDelimeter;
			  } 
		  }
		  return retVal;
	  }
	  
  }

}
