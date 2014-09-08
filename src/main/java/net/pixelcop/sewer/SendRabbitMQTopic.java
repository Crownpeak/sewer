package net.pixelcop.sewer;

import java.io.IOException;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import java.io.IOException;


public class SendRabbitMQTopic {

    private String propInput = "/src/main/resources/config.properties";

    private Properties prop = null;
    private ConnectionFactory factory;
    private Channel channel;
    private Connection connection;
    private String routingKey;

    private String EXCHANGE_NAME;
    private String EXCHANGE_TYPE;
    private String HOST_NAME;

    public SendRabbitMQTopic() {
    	loadProperties();
    	EXCHANGE_NAME = prop.getProperty("rmq.exchange.name");
    	EXCHANGE_TYPE = prop.getProperty("rmq.exchange.type");
    	HOST_NAME = prop.getProperty("rmq.host.name");
        routingKey = prop.getProperty("rmq.routing.key");

    	factory = new ConnectionFactory();
        factory.setHost(HOST_NAME);

    }

    public void sendMessage(String message) {
        try{    
            channel.basicPublish(EXCHANGE_NAME, routingKey, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    public void open() {
        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE, true); // true so its durable
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void close(){
        try {
            channel.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        } 
    }

	private void loadProperties() {
		prop = new Properties();	 
		try {
            prop.load( SendRabbitMQTopic.class.getResourceAsStream(propInput) );
			// prop.load( new FileInputStream( propInput ) );
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
}