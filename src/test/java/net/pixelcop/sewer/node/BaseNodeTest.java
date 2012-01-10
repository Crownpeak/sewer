package net.pixelcop.sewer.node;

import java.security.Permission;

import net.pixelcop.sewer.SinkRegistry;
import net.pixelcop.sewer.SourceRegistry;
import net.pixelcop.sewer.sink.debug.CountingSink;
import net.pixelcop.sewer.source.debug.EventGeneratorSource;
import net.pixelcop.sewer.source.debug.FailOpenSource;
import net.pixelcop.sewer.source.debug.NullSource;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(BlockJUnit4ClassRunner.class)
public abstract class BaseNodeTest {

  private static class NoExitSecurityManager extends SecurityManager {
    @Override
    public void checkPermission(Permission perm) {
      // allow anything.
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
      // allow anything.
    }

    @Override
    public void checkExit(int status) {
      super.checkExit(status);
      throw new ExitException(status);
    }
  }

  protected static final Logger LOG = LoggerFactory.getLogger(BaseNodeTest.class);

  private SecurityManager securityManager;

  @Before
  public void setup() throws Exception {
    securityManager = System.getSecurityManager();
    System.setSecurityManager(new NoExitSecurityManager());
    SourceRegistry.register("null", NullSource.class);
    SourceRegistry.register("gen", EventGeneratorSource.class);
    SourceRegistry.register("failopen", FailOpenSource.class);
    SinkRegistry.register("counting", CountingSink.class);
  }

  @After
  public void teardown() throws Exception {
    System.setSecurityManager(securityManager);
  }

}
