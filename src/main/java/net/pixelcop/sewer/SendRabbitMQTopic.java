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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

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
    
    private int RETRIES;

    private boolean CONFIRMS=false;

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
        
        RETRIES = Integer.parseInt(prop.getProperty("rmq.send.retries"));

        CONFIRMS = Boolean.parseBoolean( prop.getProperty("rmq.queue.is.confirm") );

    	factory = new ConnectionFactory();
        factory.setHost(HOST_NAME);
        factory.setPort(PORT_NUMBER);

        factory.setUsername(USERNAME);
        factory.setPassword(PASSWORD);
        factory.setVirtualHost(VIRTUAL_HOST);
    }

    public int sendMessage(String message, String host) {    	
        if( host.equals(ROUTING_KEY)) {
            try{    
                channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
                if( CONFIRMS) {
                    boolean test = channel.waitForConfirms();
                    if( test)
                    	return 1;
                    else
                    	return 0;
                }
            } catch (IOException e) {
                e.printStackTrace();
                LOG.warn("RABBITMQ: IOException in send message.\n"+e.getMessage());
            }  catch( InterruptedException e ) {
                 e.printStackTrace();
                 LOG.warn("RABBITMQ: InterruptException in send message.\n"+e.getMessage());
            }
            return 2;
        }
        else {
            if( LOG.isDebugEnabled() )
                LOG.debug("RABBITMQ: Event Host does not match Routing Key. Ignoring message:\n\t"+message);
            return -1;
        }
        
    }
    
    public void open() {
        try {
        	if( connection == null) {
        		LOG.info("RABBITMQ: Connection is null, creating new connection.");
        		connection = factory.newConnection();
        	}
        	else if( !connection.isOpen() ) {
        		LOG.info("RABBITMQ: Connection isn't open, creating new connection.");
        		connection = factory.newConnection();
        	}
        	
        	if( channel == null) {
        		LOG.info("RABBITMQ: Channel is null, creating new connection.");
        		channel = connection.createChannel();
                channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE, true); // true so its durable
        	}
        	else if( !channel.isOpen() ) {
        		LOG.info("RABBITMQ: Channel isn't open, creating new connection.");
        		channel = connection.createChannel();
                channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE, true); // true so its durable
        	}       

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

	@Override
	public void run() {
		// TODO Auto-generated method stub
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		while( true ) {
			if( TransactionManager.rabbitMessageQueue.size() > 0 ) {
				String fullMessage = TransactionManager.rabbitMessageQueue.get(0);
				String message = fullMessage.split(TransactionManager.rabbitMessageDelimeter)[0];
				String host = fullMessage.split(TransactionManager.rabbitMessageDelimeter)[1];
				
				int ack = -1;
//				for( int i = 0; i < RETRIES && !ack; i++) {
				open();
				ack = sendMessage(message , host);
//				}
				if(ack == 0)
                	LOG.info("RABBITMQ: Message NACKED : "+ack+"\t"+message);
				else if(ack == 1)
					TransactionManager.rabbitMessageQueue.remove(0);
				else if( ack == 2)
					LOG.info("RABBITMQ: Connection Issue when sending Messsage : "+ack+"\t"+message);
				else {
					LOG.info("RABBITMQ: Host does not match Routing Key...Ignoring: "+ack+"\t"+message);
					TransactionManager.rabbitMessageQueue.remove(0);
				}
			}
			Date dateNow = new Date();
			if((dateNow.getTime()-date.getTime())/1000 >= 15 ) {
				if(TransactionManager.rabbitMessageQueue.size() >= 3 )
					LOG.info("QueueSize: "+TransactionManager.rabbitMessageQueue.size() +"\n\t" + TransactionManager.rabbitMessageQueue.get(0) + "\n\t" + TransactionManager.rabbitMessageQueue.get(1)+"\n\t"+TransactionManager.rabbitMessageQueue.get(2));
				date = new Date();
			}
		}	
	}
}