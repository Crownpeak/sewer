package net.pixelcop.sewer.sink.durable;

import java.io.IOException;

import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.SendRabbitMQTopic;
import net.pixelcop.sewer.sink.BucketedSink;
import net.pixelcop.sewer.sink.SequenceFileWithRabbitMQSink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author richard craparotta
 *
 */
public class DeferWithRabbitMQSink extends DeferSink {

  private static final Logger LOG = LoggerFactory.getLogger(ReliableSink.class);

  private Transaction tx;
  private SequenceFileWithRabbitMQSink durableSink;

  public DeferWithRabbitMQSink(String[] args) {
	super(args);
  }

  @Override
  public void close() throws IOException {
	LOG.info("\n\n\nDEFER WITH RABBIT: CLOSING\n\n\n\n\n");
    LOG.debug("closing");
    setStatus(CLOSING);

    try {
      this.getSendRabbit().close();
      durableSink.close();
    } catch (IOException e) {
      LOG.warn("Failed to close durable sink", e);
      // will proceed with rollback anyway
    }

    tx.rollback(); // always rollback!

    setStatus(CLOSED);
    LOG.debug("closed");
  }
  
  @Override
  public void open() throws IOException {
    LOG.debug("opening");
    setStatus(OPENING);

    createSubSink(); // create but don't open

    String nextBucket = null;
    if (subSink instanceof BucketedSink) {
      nextBucket = ((BucketedSink) subSink).generateNextBucket();
    }
    this.tx = TransactionManager.getInstance().startTx(nextBucket);
    String durablePath = tx.createTxPath(false);
    this.durableSink = new SequenceFileWithRabbitMQSink(new String[] { durablePath },this);
    this.durableSink.setSendRabbit(this);
    LOG.info("Defer with RabbitMQ Sink: Set defer in sendRabbitMQTopic.");

    try {
      this.durableSink.open();
    } catch (IOException e) {
      LOG.error("Error opening durable sink at path " + durablePath, e);
      throw e;
    }

    setStatus(FLOWING);
    LOG.debug("flowing");
  }
  
  @Override
  public void append(Event event) throws IOException {
	    durableSink.append(event);
  }
  
  private SendRabbitMQTopic sendRabbit;
  public SendRabbitMQTopic getSendRabbit() {
	  return sendRabbit;
  }

}
