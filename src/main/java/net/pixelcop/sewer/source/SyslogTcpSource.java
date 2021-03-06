package net.pixelcop.sewer.source;

import java.io.IOException;
import java.net.Socket;

import net.pixelcop.sewer.ByteArrayEvent;
import net.pixelcop.sewer.Event;
import net.pixelcop.sewer.Sink;
import net.pixelcop.sewer.Source;

import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Source that listens for TCP Syslog events
 *
 * @author chetan
 *
 */
public class SyslogTcpSource extends Source {

  private static final Logger LOG = LoggerFactory.getLogger(SyslogTcpSource.class);

  public static final int SYSLOG_TCP_PORT = 514;

  private final int port;

  private TCPServerThread serverThread;

  public SyslogTcpSource() {
    this(SYSLOG_TCP_PORT);
  }

  public SyslogTcpSource(int port) {
    this.port = port;
  }

  public SyslogTcpSource(String[] args) {
    if (args == null) {
      this.port = SYSLOG_TCP_PORT;
    } else {
      this.port = NumberUtils.toInt(args[0], SYSLOG_TCP_PORT);
    }
  }

  @Override
  public void close() throws IOException {
    setStatus(CLOSING);
    LOG.info("Closing " + this.getClass().getSimpleName());
    try {
      LOG.debug("joining server thread");
      this.serverThread.join();
      LOG.debug("server thread has joined");
    } catch (InterruptedException e) {
      LOG.error("Interrupted waiting for server thread to join", e);
    }
    try {
      this.serverThread.joinReaders();
      LOG.debug("all reader threads have joined");
    } catch (InterruptedException e) {
      LOG.error("Interrupted waiting for reader threads to join", e);
    }
    setStatus(CLOSED);
  }

  @Override
  public void open() throws IOException {
    setStatus(OPENING);

    if (LOG.isInfoEnabled()) {
      LOG.info("Opening " + this.getClass().getSimpleName() + " on port " + port);
    }

    this.serverThread = new TCPServerThread("Syslog Server", port, getSinkFactory(), this) {

      @Override
      public TCPReaderThread createReader(Socket socket, Sink sink) {

        return new TCPReaderThread("Syslog Reader", socket, sink, SyslogTcpSource.this) {

          private SyslogWireExtractor reader;

          protected void createInputStream() throws IOException {
            this.reader = new SyslogWireExtractor(this.socket.getInputStream());
          };

          @Override
          public void read() throws IOException {
            Event e = reader.extractEvent();
            this.sink.append(e);
          }

        };

      }
    };

    this.serverThread.start();
    setStatus(FLOWING);
  }

  @Override
  public Class<?> getEventClass() {
    return ByteArrayEvent.class;
  }

}
