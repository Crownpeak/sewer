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
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class SendRabbitMQTopic {

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

    private boolean CONFIRMS;

    public SendRabbitMQTopic() {        
        if( LOG.isInfoEnabled() )
            LOG.ing("RABBITMQ: initializing...");
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
    }

    public void sendMessage(String message, String host) {
        if( LOG.isInfoEnabled() )
            LOG.ing("RABBITMQ: in sendMessage.");
        if( host.equals(ROUTING_KEY)) {
            try{    
                channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());

                //for testing between using confirms or norm queue
                if( CONFIRMS) {
                    if( LOG.isWarnEnabled() )
                        LOG.warn("RABBITMQ CONFIRMS: calling waitForConfirms.");
                    boolean test = channel.waitForConfirms();
                    if( LOG.isWarnEnabled() )
                        LOG.warn("RABBITMQ CONFIRMS: Return value of waitForConfirms: "+test);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }  catch( InterruptedException e ) {
                 e.printStackTrace();
            }
        }
        else {
            if( LOG.isWarnEnabled() )
                LOG.warn("RABBITMQ: Event Host does not match Routing Key. Ignoring message:\n\t"+message);
        }
    }
    
    public void open() {
        if( LOG.isInfoEnabled() )
            LOG.ing("RABBITMQ: in open.");
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
        if( LOG.isInfoEnabled() )
            LOG.ing("RABBITMQ: in close.");
        try {
            channel.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        } 
    }

	private void loadProperties() {
        if( LOG.isInfoEnabled() )
            LOG.ing("RABBITMQ: in loadProperties.");
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
}