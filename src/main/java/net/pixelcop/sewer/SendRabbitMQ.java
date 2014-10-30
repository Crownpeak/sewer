package net.pixelcop.sewer;

import java.io.IOException;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

import java.io.FileInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.Calendar;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import net.pixelcop.sewer.sink.durable.TransactionManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SendRabbitMQ extends Thread {

    private String propInput = "config.properties";
    private static final Logger LOG = LoggerFactory.getLogger(SendRabbitMQ.class);

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
        
//    public static BlockingQueue<BlockingQueue<String>> batchQueue;
	private boolean connectionError=false;
	private String path="/mnt/sewer/rabbit/";


    public SendRabbitMQ() {        
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

        createFactory();        
        start();
    }
    
    public void sendMessage() {
    	checkFolder(path);
    	String fileName = checkFile();
			
    	if( fileName != null && !connectionError ) {
//    		BlockingQueue<String> batch = batchQueue.peek();
    		File in = new File(fileName);
			FileInputStream fis;
			try {
				fis = new FileInputStream(in);
			    byte[] data = new byte[(int)in.length()];
			    fis.read(data);
			    fis.close();
			    String batch = new String(data, "UTF-8");
	    		if( batch.length() > 0) {
	    			try{
	    				if( channel==null || !channel.isOpen()) {
	    					open();
	    				}
	    	            //send message
		                channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, MessageProperties.PERSISTENT_TEXT_PLAIN, batch.getBytes());
	                    boolean acked = channel.waitForConfirms();
	                    if( acked ) {
	//                    	batchQueue.take();
	                    	in.delete();
	    	                LOG.info("RABBITMQ: Sent to Rabbit Servers, batch of Size: "+batch.split("\n").length);
	    	                connectionError=false;
	                    }
	                    else {
	    	                LOG.info("RABBITMQ: NACKED, will try resending it, left in queue.");
	                    }	                    	
		            }  catch( InterruptedException e) {
		            	e.printStackTrace();
		                connectionError=true;
		            } catch( IOException e) {
		            	e.printStackTrace();
		                connectionError=true;
		            } catch( NullPointerException e) {
		            	e.printStackTrace();
		                connectionError=true;
		            }
	    		}
	    		else {
	    			LOG.info("RABBITMQ: Batch is empty, removing from queue.");
	    			in.delete();
	    		}
			} catch (FileNotFoundException e1) {
				e1.printStackTrace();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
    	}
    }
    
    public void createFactory() {
        LOG.info("RABBITMQ: Reinitializing Connection Factory...");
    	factory = new ConnectionFactory();
        factory.setHost(HOST_NAME);
        factory.setPort(PORT_NUMBER);
        factory.setUsername(USERNAME);
        factory.setPassword(PASSWORD);
        factory.setVirtualHost(VIRTUAL_HOST);
    }
    
    public void open() throws IOException {
    	LOG.info("RABBITMQ: Opening connection with Rabbit Servers...");
		connection = factory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE, true); // true so its durable
        createQueueConfirm();
    }

    public void createQueueConfirm() {
        try {
        	LOG.info("RABBITMQ: Declaring confirms queue...");
            channel.queueDeclare(QUEUE_CONFIRM_NAME, true, false, false, null);
            channel.confirmSelect();
            channel.queueBind(QUEUE_CONFIRM_NAME, EXCHANGE_NAME, ROUTING_KEY);
        } catch (IOException e) {
        	LOG.info("RABBITMQ: Error declaring confirms queue.");
            e.printStackTrace();
        }
    }

    public void close(){
        try {
        	LOG.info("RABBITMQ: Closing connection with Rabbit Servers...");
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
        	LOG.info("RABBITMQ: Error Closing connection with Rabbit Servers.");
            e.printStackTrace();
        } 
    }

//    public void putBatch(BlockingQueue<String> queue) {
//    	try {
//    		batchQueue.put(queue);
//    		LOG.info("RABBITMQ: Batch Queue Size: "+batchQueue.size());
//    		restartConnection();
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//    }
    
    public String checkFile() {
		File folder = new File(path);
		String fileName="";
		Calendar cal=null;
		for( File f : folder.listFiles()) {
			String name = f.getName();
			name = name.replace(path,"");
			name = name.replace(".txt","");
			Calendar tempCal = Calendar.getInstance();
			int year=		Integer.parseInt( name.split("_")[0].split("-")[0] );
			int month=		Integer.parseInt( name.split("_")[0].split("-")[1] );
			int date=		Integer.parseInt( name.split("_")[0].split("-")[2] );
			int hourOfDay=	Integer.parseInt( name.split("_")[1].split(":")[0] );
			int minute=		Integer.parseInt( name.split("_")[1].split(":")[1] );
			int second=		Integer.parseInt( name.split("_")[1].split(":")[2] );
			tempCal.set(year, month, date, hourOfDay, minute, second);
			if( cal == null) {
				cal = tempCal;
				fileName=f.getName();
			}
			else if(cal.compareTo(tempCal) > 0) {
				cal=tempCal;
				fileName=f.getName();
			}
		}
		if( cal == null)
			return null;
		LOG.warn("\n\n\n\n\n\n\n\n*************************************\n\n");
		LOG.warn("RABBITMQ: BEFORE CAL: "+cal.toString());
		cal.add(cal.SECOND, 30);
		Calendar currentCal = Calendar.getInstance();
		LOG.warn("RABBITMQ: After CAL: "+cal.toString());
		LOG.warn("RABBITMQ: current_ CAL: "+currentCal.toString());
		LOG.warn("RABBITMQ: currentCal.compareTo(cal): "+currentCal.compareTo(cal)+"\n"+currentCal.get(currentCal.MINUTE) + " : "+currentCal.get(currentCal.SECOND)+" compareTo "+cal.get(cal.MINUTE) + " : "+cal.get(cal.SECOND));

		if(currentCal.compareTo(cal) > 0 ) {
			fileName=path+fileName;
			LOG.warn("RABBITMQ: This file is good to be sent to Servers: "+fileName);
			return fileName;
		}
		else {
			return null;
		}
    }
    
    public void checkFolder(String p) {
    	File folder = new File(p);
    	if( !folder.exists() ) {
    		folder.mkdirs();
    	}
    }
    
    public void restartConnection() {
    	if(connectionError){
    		try {
	            LOG.error("RABBITMQ: Connection Error, Reseting Connection...");
	            close();
	            createFactory();
				open();
	            connectionError=false;
    		} catch (IOException e) {
 				e.printStackTrace();
 			}
		}
    		 
    }
    
	private void loadProperties() {
		prop = new Properties();	 
		try {
            File file = new File(SendRabbitMQ.class.getClassLoader().getResource( propInput ).toURI());
            prop.load( new FileInputStream( file ) );
		} catch (IOException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
            e.printStackTrace();
        }
	}
	
	public void run() {
//		if(batchQueue == null) {
//        	LOG.info("RABBITMQ: Initializing batchQueue...");
//			batchQueue = new LinkedBlockingQueue<BlockingQueue<String>>();
//		}
		while(true) {
			sendMessage();
		}
	}
	
}