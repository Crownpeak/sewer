package net.pixelcop.sewer;

import java.io.IOException;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

import net.pixelcop.sewer.sink.durable.TransactionManager;

import org.eclipse.jetty.util.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SendRabbitMQTopic extends Thread {

    private String propInput = "config.properties";
    private static final Logger LOG = LoggerFactory.getLogger(SendRabbitMQTopic.class);

    private Properties prop = new Properties();
    private ConnectionFactory factory;
    private Channel channel;
    private Connection connection;

    private String EXCHANGE_NAME;
    private String EXCHANGE_TYPE;
    private String HOST_NAME;
    private int PORT_NUMBER;
    private String ROUTING_KEY;

    private String USERNAME;
    private String PASSWORD;
    private String VIRTUAL_HOST;

    private String QUEUE_NAME;
    private String QUEUE_CONFIRM_NAME;

    private boolean CONFIRMS=false;
    
//    public static LinkedBlockingQueue<String> testArray = new LinkedBlockingQueue<String>();
//    public final String testDelimeter = ":::";
    
    public static LinkedBlockingQueue<RabbitMessageBatch> batchQueue = new LinkedBlockingQueue<RabbitMessageBatch>();

    public SendRabbitMQTopic() {        
    	loadProperties();
    	EXCHANGE_NAME = prop.getProperty("rmq.exchange.name");
    	EXCHANGE_TYPE = prop.getProperty("rmq.exchange.type");
    	HOST_NAME = prop.getProperty("rmq.host.name");
        PORT_NUMBER = Integer.parseInt( prop.getProperty("rmq.port.number") );
        ROUTING_KEY = prop.getProperty("rmq.routing.key");

        USERNAME = prop.getProperty("rmq.username");
        PASSWORD = prop.getProperty("rmq.password");
        VIRTUAL_HOST = prop.getProperty("rmq.virtual.host");

        QUEUE_NAME = prop.getProperty("rmq.queue.name");
        QUEUE_CONFIRM_NAME = prop.getProperty("rmq.queue.confirm.name");

        CONFIRMS = Boolean.parseBoolean( prop.getProperty("rmq.queue.is.confirm") );

    	factory = new ConnectionFactory();
        factory.setHost(HOST_NAME);
        factory.setPort(PORT_NUMBER);

        factory.setUsername(USERNAME);
        factory.setPassword(PASSWORD);
        factory.setVirtualHost(VIRTUAL_HOST);
        
        start();
    }
    
    public void sendMessage() {
    	if( batchQueue.size() > 0 ) {
    		RabbitMessageBatch rmb = batchQueue.peek();
	    	
	        if( rmb.getHost().equals(ROUTING_KEY)) {
	            try{
	                channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, MessageProperties.PERSISTENT_TEXT_PLAIN, rmb.getBatch().getBytes());
	                if( CONFIRMS) {
	                    boolean test = channel.waitForConfirms();
	                    if( test ) {
	    		            batchQueue.take();
	    		    		LOG.info("RABBITMQ: \tSENT: Current Size of queue is: "+batchQueue.size());
	    		    		LOG.info("RABBITMQ:\n\t# of Messages in batch: "+rmb.getCount()+"\n\n");
	                    }
	                    else {
	    	                LOG.info("RABBITMQ: Was NACKED, Try resending, leave in queue.");
	                    }	                    	
	                }
	            } catch (IOException e) {
	                e.printStackTrace();
	                LOG.info("RABBITMQ: ConnectionError, Try resending, leave in queue.");
	            }  catch( InterruptedException e ) {
	                 e.printStackTrace();
	                 LOG.info("RABBITMQ: ConnectionError, Try resending, leave in queue.");
	            }
	        }
	        else {
//	            if( LOG.isDebugEnabled() )
                LOG.info("RABBITMQ: Event Host does not match Routing Key. Ignoring message. Current Size of queue is: "+batchQueue.size());
	    		LOG.info("RABBITMQ:\n\t# of Messages in INVAID batch: -"+rmb.getCount()+"\n\n");

	            try {
					batchQueue.take();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
	        }
    	}

    }
    
    public void putBatch(RabbitMessageBatch rmb) {
    	try {
			batchQueue.put(rmb);
    		LOG.info("RABBITMQ: ADDED: Current Size of queue is: "+batchQueue.size());
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }
    
    public void open() {
        try {
    		connection = factory.newConnection();
            channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE, true); // true so its durable

            //test code for easy switching between confirms queue and normal queue
            if(CONFIRMS)
                createQueueConfirm();
            else
                createQueue();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createQueueConfirm() {
        try {
            channel.queueDeclare(QUEUE_CONFIRM_NAME, true, false, false, null);
            channel.confirmSelect();
            channel.queueBind(QUEUE_CONFIRM_NAME, EXCHANGE_NAME, ROUTING_KEY);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void createQueue() {
        try {
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close(){
        try {
        	if( channel != null )
        		if( channel.isOpen() )
        			channel.close();
        		else
        			LOG.error("RABBITMQ: Channel is already closed.");
        	else
        		LOG.error("RABBITMQ: Channel is null.");
        	if( connection != null)
        		if( connection.isOpen() )
        			connection.close();
        		else
        			LOG.error("RABBITMQ: Connection is already closed.");
        	else
        		LOG.error("RABBITMQ: Connection is null.");
        } catch (IOException e) {
            e.printStackTrace();
        } 
    }

	private void loadProperties() {
		prop = new Properties();	 
		try {
            File file = new File(SendRabbitMQTopic.class.getClassLoader().getResource( propInput ).toURI());
            prop.load( new FileInputStream( file ) );
		} catch (IOException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
            e.printStackTrace();
        }
	}
	
	public void run() {
	    TransactionManager.sendRabbit.open();
		while(true) {
			sendMessage();
		}
	}
	
	public class RabbitMessageBatch {
		
		String batch=null;
		String host="";
		int count=0;
				
		public RabbitMessageBatch(String host) {
			this.host = host;
		}
		
		public boolean checkHostAndAddMessage(String message, String host) {
			if(this.host.equals(host)) {
				if(this.batch==null) 
					this.batch=message;
				else
					this.batch+="\n"+message;	
				count++;
				return true;
			}
			else
				return false;
		}
		
		public String getBatch() {
			return this.batch;
		}
		
		public String getHost() {
			return host;
		}
		
		public int getCount() {
			return count;
		}
	}
	
}