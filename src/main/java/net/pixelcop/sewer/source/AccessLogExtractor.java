package net.pixelcop.sewer.source;

import net.pixelcop.sewer.ByteArrayEvent;
import net.pixelcop.sewer.Event;

import org.apache.hadoop.io.Text;
import org.eclipse.jetty.http.HttpHeaders;
import org.eclipse.jetty.server.Request;

public class AccessLogExtractor {

  private static final byte[] TAB = new String("\t").getBytes();
  private static final byte[] S_204 = new String("204").getBytes();

  public static Event extractByteArrayEvent(Request baseRequest) {

    Text text = new Text();

    // 9 vals
    // ${time_nsecs},${ip_remote},"${http_host}","${request}","${query_string}"
    // ${status},"${http_referrer}","${http_user_agent}","${http_cookie}"

    // time_nsecs
    // we currently want ns, but we might be better off with
    // using the already stored ms in baseRequest.getTimeStamp()
    append(text, Long.toString(System.nanoTime()));

    // ip_remote
    String addr = baseRequest.getHeader(HttpHeaders.X_FORWARDED_FOR);
    if (addr == null) {
        addr = baseRequest.getRemoteAddr();
    }
    append(text, addr);

    // http_host
    append(text, baseRequest.getServerName());

    // request
    append(text, baseRequest.getRequestURI());

    // query_string
    append(text, baseRequest.getQueryString());

    // status
    text.append(S_204, 0, 3);
    text.append(TAB, 0, 1);

    // http_referrer
    append(text, baseRequest.getHeader(HttpHeaders.REFERER));

    // http_user_agent
    append(text, baseRequest.getHeader(HttpHeaders.USER_AGENT));

    // http_cookie
    append(text, baseRequest.getHeader(HttpHeaders.COOKIE), true);

    return new ByteArrayEvent(text.toString().getBytes());
  }

  private static void append(Text text, String val) {
    append(text, val, false);
  }

  private static void append(Text text, String val, boolean lastVal) {
    if (val != null) {
      text.append(val.getBytes(), 0, val.length());
    }
    text.append(TAB, 0, 1);
  }

}