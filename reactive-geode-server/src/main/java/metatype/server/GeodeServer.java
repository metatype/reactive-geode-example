package metatype.server;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes.EncodedValue;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol.Message;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI.GetResponse;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI.PutResponse;

import com.google.protobuf.NullValue;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

public class GeodeServer {
  public static void main(String[] args) throws Exception {
    int port = args.length == 0 ? 31415 : Integer.parseInt(args[0]);
    new GeodeServer(port).listen();
  }

  private final int port;
  private final ConcurrentHashMap<Long, String> data;
  
  public GeodeServer(int port) {
    this.port = port;
    this.data = new ConcurrentHashMap<>();
  }
  
  public void listen() throws InterruptedException {
    EventLoopGroup server = new NioEventLoopGroup();
    EventLoopGroup worker = new NioEventLoopGroup();
    
    try {
      ServerBootstrap bootStrap = new ServerBootstrap()
          .group(server, worker)
          .channel(NioServerSocketChannel.class)
          .option(ChannelOption.SO_BACKLOG, 128)
          .childOption(ChannelOption.SO_SNDBUF, 64 * 1024)
          .childOption(ChannelOption.SO_RCVBUF, 64 * 1024)
          .childOption(ChannelOption.TCP_NODELAY, true)
          .handler(new LoggingHandler(LogLevel.INFO))
          .childHandler(new GeodeServerInitializer());
      
      bootStrap.bind(port).sync().channel().closeFuture().sync();
      
    } finally {
      server.shutdownGracefully();
      worker.shutdownGracefully();
    }
  }
  
  private class GeodeServerInitializer extends ChannelInitializer<SocketChannel> {
    @Override
    protected void initChannel(SocketChannel channel) throws Exception {
      channel.pipeline()
      .addLast(new ProtobufVarint32FrameDecoder())
      .addLast(new ProtobufDecoder(ClientProtocol.Message.getDefaultInstance()))
      .addLast(new ProtobufVarint32LengthFieldPrepender())
      .addLast(new ProtobufEncoder())
// need to deal with read timeouts, e.g.  .addLast(new ReadTimeoutHandler(timeout, unit))
      .addLast(new IncomingMessageHandler())
      .addLast(new ExceptionHandler());
    }
  }
  
  @Sharable
  private class IncomingMessageHandler extends SimpleChannelInboundHandler<ClientProtocol.Message> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg)
        throws Exception {

      // So...we're doing all the data updates on the worker threads.  Just sayin'
      if (msg.hasGetRequest()) {
        long key = msg.getGetRequest().getKey().getLongResult();
        String value = data.get(key);
        
        ctx.channel().writeAndFlush(encodeGet(value));
        
      } else if (msg.hasPutRequest()) {
        long key = msg.getPutRequest().getEntry().getKey().getLongResult();
        String value = msg.getPutRequest().getEntry().getValue().getStringResult();
        
        data.put(key, value);
        ctx.channel().writeAndFlush(encodePut());

      } else {
        throw new RuntimeException("oops!");
      }
    }

    private Message encodePut() {
      return Message.newBuilder()
          .setPutResponse(PutResponse.getDefaultInstance())
          .build();
    }

    private Message encodeGet(String value) {
      EncodedValue result = 
          (value == null ? 
              EncodedValue.newBuilder().setNullResult(NullValue.NULL_VALUE) :
              EncodedValue.newBuilder().setStringResult(value))
          .build();
      
      return Message.newBuilder()
          .setGetResponse(GetResponse.newBuilder().setResult(result))
          .build();
    }
  }
  
  @Sharable
  private static class ExceptionHandler extends ChannelDuplexHandler {
    private final ChannelFutureListener closeAfterError = future -> {
      if (!future.isSuccess()) {
        future.cause().printStackTrace();
        future.channel().close();
      }
    };
    
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
      ctx.write(msg, promise.addListener(closeAfterError));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
      cause.printStackTrace();
      ctx.close();
    }
  }
}
