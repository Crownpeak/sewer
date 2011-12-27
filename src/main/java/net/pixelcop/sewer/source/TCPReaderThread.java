package net.pixelcop.sewer.source;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;

import net.pixelcop.sewer.Sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TCPReaderThread extends Thread {

  private static final Logger LOG = LoggerFactory.getLogger(TCPReaderThread.class);

  protected Socket socket;
  protected Sink sink;
  protected DataInputStream in;

  public TCPReaderThread(Socket socket, Sink sink) {
    this.socket = socket;
    this.sink = sink;
  }

  @Override
  public void run() {

    try {

      createInputStream();
      while (true) {
        read();
      }

    } catch (EOFException e) {
      LOG.info("Client closed..");

    } catch (IOException e) {
      LOG.warn("IO Exception in ReaderThread", e);

    } finally {
      try {
        sink.close();
      } catch (Exception e) {
        // TODO properly handle these errors - rollback??
        LOG.warn("Error closing sink during shutdown");
      }
      try {
        socket.close();
      } catch (IOException e) {
        LOG.info("Error closing socket during shutdown");
      }

    }

  }

  protected void createInputStream() throws IOException {

    this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 64 * 1024));
    // TODO: enable snappy compression
    // new SnappyCodec().createInputStream(in);

  }

  public abstract void read() throws IOException;

}