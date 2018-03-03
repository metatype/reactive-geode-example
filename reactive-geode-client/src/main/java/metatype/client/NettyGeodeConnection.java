package metatype.client;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;

import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes.EncodedValue;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes.Entry;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol.ErrorResponse;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol.Message;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI.GetRequest;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI.PutRequest;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import metatype.client.GeodeClient.GeodeClientException;
import metatype.client.GeodeClient.GeodeConnection;
import metatype.client.GeodeClient.GeodeResult;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

public class NettyGeodeConnection implements GeodeConnection {
  private final ConcurrentLinkedQueue<GeodeResult<?>> requests;
  private final Channel channel;
  
  private final ChannelFutureListener cleanupAfterError = future -> {
    if (!future.isSuccess()) {
      close(future.cause());
    }
  };
  
  public NettyGeodeConnection(Channel channel) {
    requests = new ConcurrentLinkedQueue<>();
    this.channel = channel;
    
    channel.pipeline()
        .addLast(new ProtobufVarint32FrameDecoder())
        .addLast(new ProtobufDecoder(ClientProtocol.Message.getDefaultInstance()))
        .addLast(new ProtobufVarint32LengthFieldPrepender())
        .addLast(new ProtobufEncoder())
// need to deal with read timeouts, e.g.  .addLast(new ReadTimeoutHandler(timeout, unit))
        .addLast(new IncomingMessageHandler())
        .addLast(new ExceptionHandler());
  }

  @Override
  public void close() throws Exception {
    close(new IOException("Connection closed"));
  }

  @Override
  public <K,V> void asyncGet(String region, K key, GeodeResult<V> result) {
    sendRequest(encodeGet(region, key), result);
  }

  @Override 
  public <K,V> V syncGet(String region, K key) throws InterruptedException, ExecutionException {
    CompletableFuture<V> done = new CompletableFuture<>();
    asyncGet(region, key, callbackToFuture(done));
    
    return done.get();
  }
  
  @Override 
  public <K,V> Mono<V> rxGet(String region, K key) {
    return Mono.create(sink -> {
      asyncGet(region, key, callbackToSink(sink));
    });
  }
  
  @Override
  public <K,V> void asyncPut(String region, K key, V value, GeodeResult<Void> result) {
    sendRequest(encodePut(region, key, value), result);
  }

  @Override
  public <K,V> void syncPut(String region, K key, V value) throws InterruptedException, ExecutionException {
    CompletableFuture<Void> done = new CompletableFuture<>();
    asyncPut(region, key, value, callbackToFuture(done));
    
    done.get();
  }

  @Override
  public <K,V> Mono<Void> rxPut(String region, K key, V value) {
    return Mono.create(sink -> {
      asyncPut(region, key, value, callbackToSink(sink));
    });
  }

  private <V> GeodeResult<V> callbackToSink(MonoSink<V> sink) {
    return new GeodeResult<V>() {
      @Override
      public void onFailure(Throwable t) {
        sink.error(t);
      }
    
      @Override
      public void onSuccess(V result) {
        sink.success(result);
      }
    
      @Override
      public void onError(int code, String msg) {
        sink.error(new GeodeClientException(code, msg));
      }
    };
  }
  
  private <V> GeodeResult<V> callbackToFuture(CompletableFuture<V> signal) {
    return new GeodeResult<V>() {
      @Override
      public void onFailure(Throwable t) {
        signal.completeExceptionally(t);
      }

      @Override
      public void onSuccess(V result) {
        signal.complete(result);
      }

      @Override
      public void onError(int code, String msg) {
        signal.completeExceptionally(new GeodeClientException(code, msg));
      }
    };
  }
  
  private <K> Message encodeGet(String region, K key) {
    // it's a little weird that the request has an error field
    Message request = Message.newBuilder()
        .setGetRequest(GetRequest.newBuilder()
            .setRegionName(region)
            .setKey(encodeKey((Long) key)) // <- yuck
            .build())
        .build();
    return request;
  }

  private <K, V> Message encodePut(String region, K key, V value) {
    return Message.newBuilder()
        .setPutRequest(PutRequest
            .newBuilder()
              .setRegionName(region)
              .setEntry(Entry.newBuilder()
                  .setKey(encodeKey((Long) key)) // <- yuck
                  .setValue(encodeValue((String) value))
                  .build())
              .build())
        .build();
  }

  private EncodedValue encodeValue(String value) {
    return EncodedValue.newBuilder().setStringResult(value).build();
  }

  private EncodedValue encodeKey(long key) {
    return EncodedValue.newBuilder().setLongResult(key).build();
  }

  private <R> void sendRequest(Message request, GeodeResult<R> result) {
    requests.add(result);
    channel.writeAndFlush(request).addListener(cleanupAfterError);
  }
  
  private void close(Throwable t) {
    channel.close();
    requests.forEach(callback -> callback.onFailure(t));
  }
  
  @Sharable
  private class IncomingMessageHandler extends SimpleChannelInboundHandler<ClientProtocol.Message> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Message msg)
        throws Exception {

      GeodeResult<?> result = requests.remove();
      
      if (msg.hasErrorResponse()) {
        ErrorResponse err = msg.getErrorResponse();
        result.onError(err.getError().getErrorCodeValue(), err.getError().getMessage());
        
      } else if (msg.hasGetResponse()) {
        String value = msg.getGetResponse().getResult().getValueCase() == EncodedValue.ValueCase.NULLRESULT ?
            null : msg.getGetResponse().getResult().getStringResult();
        ((GeodeResult<String>) result).onSuccess(value);
        
      } else if (msg.hasPutResponse()) {
        result.onSuccess(null);
        
      } else {
        throw new RuntimeException("oops!");
      }
    }
  }
  
  @Sharable
  private class ExceptionHandler extends ChannelDuplexHandler {
    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
      ctx.write(msg, promise.addListener(cleanupAfterError));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
            throws Exception {
      ctx.close();
      NettyGeodeConnection.this.close(cause);
    }
  }
}