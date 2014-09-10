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

    public SendRabbitMQTopic() {
        if (LOG.isInfoEnabled())
            LOG.info(":::START RMQ Constructor");
        
    	loadProperties();
    	EXCHANGE_NAME = prop.getProperty("rmq.exchange.name");
    	EXCHANGE_TYPE = prop.getProperty("rmq.exchange.type");
    	HOST_NAME = prop.getProperty("rmq.host.name");
        PORT_NUMBER = Integer.parseInt( prop.getProperty("rmq.port.number") );
        ROUTING_KEY = prop.getProperty("rmq.routing.key");

        USERNAME = prop.getProperty("rmq.username");
        PASSWORD = prop.getProperty("rmq.password");
        VIRTUAL_HOST = prop.getProperty("rmq.virtual.host");

    	factory = new ConnectionFactory();
        factory.setHost(HOST_NAME);
        factory.setPort(PORT_NUMBER);

        factory.setUsername(USERNAME);
        factory.setPassword(PASSWORD);
        factory.setVirtualHost(VIRTUAL_HOST);
        if (LOG.isInfoEnabled())
            LOG.info(":::END RMQ Constructor");

    }

    public void sendMessage(String message) {
        if (LOG.isInfoEnabled())
            LOG.info(":::START RMQ sendMessage");
        try{    
            channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
            if (LOG.isInfoEnabled())
                LOG.info(":::END RMQ sendMessage");
        } catch (IOException e) {
            e.printStackTrace();
        }  
    }
    
    public void open() {
        if (LOG.isInfoEnabled())
            LOG.info(":::START RMQ open");
        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE, true); // true so its durable
            if (LOG.isInfoEnabled())
                LOG.info(":::END RMQ open");
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public void close(){
        if (LOG.isInfoEnabled())
            LOG.info(":::START RMQ close");
        try {
            channel.close();
            connection.close();
            if (LOG.isInfoEnabled())
                LOG.info(":::END RMQ close");
        } catch (IOException e) {
            e.printStackTrace();
        } 
    }

	private void loadProperties() {
        if (LOG.isInfoEnabled())
            LOG.info(":::START RMQ loadProperties");
		prop = new Properties();	 
		try {
            File file = new File(SendRabbitMQTopic.class.getClassLoader().getResource( propInput ).toURI());
            prop.load( new FileInputStream( file ) );
            if (LOG.isInfoEnabled())
                LOG.info(":::END RMQ loadProperties");
		} catch (IOException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
            e.printStackTrace();
        }
	}
}