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
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import kafka.producer.ProducerConfig;
/**
 * Created by aabraham on 8/4/14.
 */
public class KafkaSink {
    private final Producer<String, Message> kafkaProducer;
    private static final SpecificDatumWriter<Event> avroEventWriter = new SpecificDatumWriter<Event>(Event.SCHEMA$);
    private static final EncoderFactory avroEncoderFactory = EncoderFactory.get();

    public KafkaAvroPublisherExample(MyKafaSettings settings) {
        Properties props = new Properties();
        props.put("zk.connect", settings.zookeeper);
        props.put("serializer.class", "kafka.serializer.DefaultEncoder");
        props.put("compression.codec", settings.compression); // snappy
        props.put("message.send.max.retries", settings.maxRetry);
        props.put("batch.num.messages", settings.batchSize);
        props.put("client.id", settings.applicationId);
        kafkaProducer = new Producer<String, Message>(new ProducerConfig(props));
    }

    public void publish(Event event) {
        try {
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            BinaryEncoder binaryEncoder = encoderFactory.binaryEncoder(stream, null);
            avroEventWriter.write(event, binaryEncoder);
            binaryEncoder.flush();
            IOUtils.closeQuietly(stream);

            Message m = new Message(stream.toByteArray());
            producer.send(new ProducerData<String, Message>("rawdata", Lists.newArrayList(m)));
        } catch (IOException e) {
            throw new RuntimeException("Avro serialization failure", e);
        }
    }
}
}
