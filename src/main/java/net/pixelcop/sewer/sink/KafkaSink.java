// package net.pixelcop.sewer.sink;

// import java.io.ByteArrayOutputStream;
// import java.io.IOException;
// import java.io.InputStream;
// import java.util.*;

// import org.apache.avro.io.BinaryDecoder;
// import org.apache.avro.io.BinaryEncoder;
// import org.apache.avro.io.DecoderFactory;
// import org.apache.avro.io.EncoderFactory;
// import org.apache.avro.specific.SpecificDatumReader;
// import org.apache.avro.specific.SpecificDatumWriter;
// import org.apache.avro.util.ByteBufferInputStream;
// import org.apache.commons.io.IOUtils;

// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;

// import kafka.consumer.Consumer;
// import kafka.consumer.ConsumerConfig;
// import kafka.consumer.KafkaStream;
// import kafka.javaapi.consumer.ConsumerConnector;
// import kafka.javaapi.producer.Producer;
// import kafka.message.Message;
// import kafka.message.MessageAndMetadata;
// import kafka.producer.KeyedMessage;
// import kafka.producer.ProducerConfig;
// import kafka.serializer.Encoder;
// import net.pixelcop.sewer.Event;
// import net.pixelcop.sewer.source.http.AccessLogEvent;


// public class KafkaSink {


//     private static final SpecificDatumWriter<Event> avroEventWriter = new SpecificDatumWriter<Event>(AccessLogEvent.SCHEMA$);
//     private static final AvroKafkaEncoder avroEncoderFactory = EncoderFactory.get();

//     private final Producer<String, Message> producer;

//     public void setProducer(Producer<String, Message> producer) {
//         this.producer = producer;
//     }


//     public void getSettings(String settings) {
//         Properties props = new Properties();
//         props.put("broker.list", "kafka01:9092,kafka02:9092,kafka03:9092");
//         props.put("serializer.class", "kafka.serializer.DefaultEncoder");
//         props.put("producer.type", "async");
//         props.put("request.required.acks", "1");
//         props.put("queue.enqueue.timeout.ms", "-1");
//         props.put("batch.num.messages", "300");
//         props.put("compression.codec", "snappy");
//         props.put("message.send.max.retries", "3");
//         this.settings = settings;
//     }

//     public void publish(AccessLogEvent event) {
//         try {
//             ByteArrayOutputStream stream = new ByteArrayOutputStream();
//             BinaryEncoder binaryEncoder = encoderFactory.binaryEncoder(stream, null);
//             avroEventWriter.write(event, binaryEncoder);
//             binaryEncoder.flush();
//             IOUtils.closeQuietly(stream);

//             Message m = new Message(stream.toByteArray());
//             producer.send(new ProducerRecord("rawdata", "value")).get();

//         } catch (IOException e) {
//             throw new RuntimeException("Exception caught", e);
//         }
//     }
// }