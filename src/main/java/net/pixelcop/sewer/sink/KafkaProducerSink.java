package net.pixelcop.sewer.sink;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.commons.io.IOUtils;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


public class KafkaProducerSink {
    private final Producer<String, Message> kafkaProducer;
    private static final SpecificDatumWriter<kEvent> avroEventWriter = new SpecificDatumWriter<kEvent>(kEvent.SCHEMA$);
    private static final EncoderFactory avroEncoderFactory = EncoderFactory.get();


    public KafkaAvroPublisher(String settings) {
        Properties props = new Properties();
//        props.put("broker.list", "kafka01:9092,kafka02:9092,kafka03:9092"); // if broker list is used
        props.put(“zk.connect”, “127.0.0.1:2181,10.10.0.10:2181”)
        props.put("serializer.class", "kafka.serializer.DefaultEncoder");
        props.put("producer.type", "async");
        props.put("request.required.acks", "1");
        props.put("queue.enqueue.timeout.ms", "-1");
        props.put("batch.num.messages", "300");
        props.put("compression.codec", "snappy");
        props.put("message.send.max.retries", "3");
        kafkaProducer = new Producer<String, Message>(new ProducerConfig(props));
        this.settings = settings;
    }


    public void publish(kEvent event) {
        try {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            BinaryEncoder binaryEncoder = encoderFactory.binaryEncoder(stream, null);
            avroEventWriter.write(event, binaryEncoder);
            binaryEncoder.flush();
            IOUtils.closeQuietly(stream);

            Message m = new Message(stream.toByteArray());
            producer.send(new ProducerRecord("rawdata", "key", "value")).get();
        } catch (IOException e) {
            throw new RuntimeException("Avro serialization failure", e);
        }
    }
}
