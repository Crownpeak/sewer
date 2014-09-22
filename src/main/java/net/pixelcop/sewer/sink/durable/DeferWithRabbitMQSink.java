package net.pixelcop.sewer.sink.durable;

import java.io.IOException;

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
    this.durableSink = new SequenceFileWithRabbitMQSink(new String[] { durablePath });

    try {
      this.durableSink.open();
    } catch (IOException e) {
      LOG.error("Error opening durable sink at path " + durablePath, e);
      throw e;
    }

    setStatus(FLOWING);
    LOG.debug("flowing");
  }

}
