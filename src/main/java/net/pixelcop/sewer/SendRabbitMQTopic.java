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
    	EXCHANGE_NAME = prop.load("rmq.exchange.name");
    	EXCHANGE_TYPE = prop.load("rmq.exchange.type");
    	HOST_NAME = prop.load("rmq.host.name");
        routingKey = prop.load("rmq.routing.key");

    	factory = new ConnectionFactory();
        factory.setHost(HOST_NAME);

    }

    public void sendMessage(String message) {
        channel.basicPublish(EXCHANGE_NAME, routingKey, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
    }
    
    public void open() {
        connection = factory.newConnection();
        channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE, true); // true so its durable
    }

    public void close(){
    	channel.close();
        connection.close();
    }

	private void loadProperties() {
		prop = new Properties();	 
		try {
			prop.load( new FileInputStream( propInput ) );
		} catch (IOException e) {
			e.printStackTrace();
		} 
	}
}