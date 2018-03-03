package metatype.rxapp;

import static org.springframework.web.reactive.function.BodyInserters.fromObject;
import static org.springframework.web.reactive.function.server.RequestPredicates.GET;
import static org.springframework.web.reactive.function.server.RequestPredicates.PUT;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;

import metatype.client.GeodeClient;
import metatype.client.GeodeClient.GeodeConnection;
import metatype.client.NettyGeodeClient;
import reactor.core.publisher.Mono;

@SpringBootApplication
public class GeodeRxApp {
  public static void main(String[] args) {
    SpringApplication.run(GeodeRxApp.class, args);
  }

  @Bean
  public GeodeClient client() {
    return new NettyGeodeClient();
  }
  
  @Bean 
  public GeodeConnection connect(GeodeClient client) throws InterruptedException, ExecutionException {
    return client.syncConnect("localhost", 31415, 0, TimeUnit.MILLISECONDS);
  }
  
  @Bean
  public RouterFunction<ServerResponse> routes(RequestHandler handler) {
    return route(GET("/get/{key}"), handler::get)
        .andRoute(PUT("/put/{key}"), handler::put);
  }
}

@Component
class RequestHandler {
  private final GeodeConnection connection;
  
  public RequestHandler(GeodeConnection connection) {
    this.connection = connection;
  }
  
  public Mono<ServerResponse> get(ServerRequest request) {
    long key = Long.valueOf(request.pathVariable("key"));
    return connection.rxGet("ignore", key)
        .flatMap(value -> ServerResponse.ok()
            .contentType(MediaType.TEXT_PLAIN)
            .body(fromObject(value)))
        .doOnError(ex -> ServerResponse.status(500)
            .contentType(MediaType.TEXT_PLAIN)
            .body(fromObject(ex.getMessage())));
  }
  
  public Mono<ServerResponse> put(ServerRequest request) {
    long key = Long.valueOf(request.pathVariable("key"));
    return request.bodyToMono(String.class).defaultIfEmpty("")
        .flatMap(value -> connection.rxPut("ignore", key, value))
        .flatMap(result -> ServerResponse.ok().build())
        .doOnError(ex -> ServerResponse.status(500)
            .contentType(MediaType.TEXT_PLAIN)
            .body(fromObject(ex.getMessage())));
  }
}
