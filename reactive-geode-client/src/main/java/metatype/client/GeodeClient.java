package metatype.client;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import reactor.core.publisher.Mono;

public interface GeodeClient extends AutoCloseable {
  void asyncConnect(String host, int port, long timeout, TimeUnit unit,
      GeodeCallback<GeodeConnection> result);

  GeodeConnection syncConnect(String host, int port, long timeout,
      TimeUnit unit) throws InterruptedException, ExecutionException;

  Mono<GeodeConnection> rxConnect(String host, int port, long timeout,
      TimeUnit unit);
  
  interface GeodeConnection extends AutoCloseable {
    <K,V> void asyncGet(String region, K key, GeodeResult<V> result);
    <K,V> V syncGet(String region, K key) throws InterruptedException, ExecutionException;
    <K,V> Mono<V> rxGet(String region, K key);
    
    <K,V> void asyncPut(String region, K key, V value, GeodeResult<Void> result);
    <K,V> void syncPut(String region, K key, V value) throws InterruptedException, ExecutionException;
    <K,V> Mono<Void> rxPut(String region, K key, V value);
  }
  
  interface GeodeCallback<R> {
    void onFailure(Throwable t);
    void onSuccess(R result);
  }
  
  interface GeodeResult<R> extends GeodeCallback<R> {
    void onError(int code, String msg);
  }
  
  public class GeodeClientException extends Exception {
    private final int code;
    
    public GeodeClientException(int code, String msg) {
      super(msg);
      this.code = code;
    }
    
    public int getErrorCode() {
      return code;
    }
  }
}
