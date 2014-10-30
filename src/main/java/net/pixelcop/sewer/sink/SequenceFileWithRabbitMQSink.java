package net.pixelcop.sewer.sink;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Calendar;
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
  
  private boolean newFile=true;
  private Calendar cal;
  private String fileName="";
  private String path="/mnt/sewer/rabbit/";

  private PrintWriter writer=null;

  public SequenceFileWithRabbitMQSink(String[] args) {
	super(args);
  }

  @Override
  public void close() throws IOException {
	  if( writer != null) {
		writer.flush();
	  	writer.close();
	  }
	  if( !TransactionManager.sendRabbit.isAlive() )
		  TransactionManager.restartRabbit();
	  LOG.info("RABBITMQ: Sending batch of Size: "+batch.size());
//	  TransactionManager.sendRabbit.putBatch(batch);
	  super.close();
  }
  
  @Override
  public void open() throws IOException {
	  newFile=true;
	  cal = Calendar.getInstance();
	  TransactionManager.sendRabbit.checkFolder(path);
	  super.open();
  }
    
  @Override
  public void append(Event event) throws IOException {
    super.append(event);
    if( LOG.isDebugEnabled() )
		LOG.info("RABBITMQ: Appending Message: "+event.toString());
    if( newFile) {
		if(writer != null)
			writer.close();
		fileName= path+cal.get(Calendar.YEAR)+"-"+(cal.get(Calendar.MONTH)+1)+"-"+cal.get(Calendar.DAY_OF_MONTH)+"_"+cal.get(Calendar.HOUR_OF_DAY)+":"+cal.get(Calendar.MINUTE)+":";
		if( cal.get(Calendar.SECOND) < 30)
			fileName+="00.txt";
		else
			fileName+="30.txt";
		LOG.info("RABBITMQ: File Created: "+ fileName);
		try {
			writer = new PrintWriter(fileName, "UTF-8");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		writer.write(event.toString());
		writer.flush();
		newFile=false;
	}
	else {
		writer.write("\n"+event.toString());
		writer.flush();
	}
//    	if( batch.size() == 0)
//    		batch.put(event.toString());
//       	else
//    		batch.put("\n"+event.toString());
  }

}
