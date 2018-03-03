package metatype.client;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import reactor.core.publisher.Mono;

public class NettyGeodeClient implements GeodeClient {
  private final EventLoopGroup group;
  
  public NettyGeodeClient() {
    group = new NioEventLoopGroup();
  }
  
  @Override
  public void asyncConnect(String host, int port, long timeout, TimeUnit unit, GeodeCallback<GeodeConnection> result) {
    Bootstrap bootstrap = new Bootstrap()
        .group(group)
        .channel(NioSocketChannel.class)
        .option(ChannelOption.SO_SNDBUF, 64 * 1024)
        .option(ChannelOption.SO_RCVBUF, 64 * 1024)
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) unit.toMillis(timeout))
        .handler(new LoggingHandler(LogLevel.INFO));

    bootstrap.connect(host, port).addListener(future -> {
      ChannelFuture done = (ChannelFuture) future;
      if (!done.isSuccess()) {
        result.onFailure(done.cause());
      }

      result.onSuccess(new NettyGeodeConnection(done.channel()));
    });
  }
  
  @Override
  public GeodeConnection syncConnect(String host, int port, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException {
    CompletableFuture<GeodeConnection> done = new CompletableFuture<>();
    asyncConnect(host, port, timeout, unit, new GeodeCallback<GeodeClient.GeodeConnection>() {
      @Override
      public void onFailure(Throwable t) {
        done.completeExceptionally(t);
      }

      @Override
      public void onSuccess(GeodeConnection result) {
        done.complete(result);
      }
    });
    return done.get();
  }

  @Override
  public Mono<GeodeConnection> rxConnect(String host, int port, long timeout, TimeUnit unit) {
    return Mono.create(sink -> {
      asyncConnect(host, port, timeout, unit, new GeodeCallback<GeodeClient.GeodeConnection>() {
        @Override
        public void onFailure(Throwable t) {
          sink.error(t);
        }
      
        @Override
        public void onSuccess(GeodeConnection result) {
          sink.success();
        }
      });
    });
  }

  @Override
  public void close() {
    group.shutdownGracefully().syncUninterruptibly();
  }
}
