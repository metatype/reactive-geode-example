package metatype.client;

import java.util.Arrays;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;

import metatype.client.GeodeClient.GeodeCallback;
import metatype.client.GeodeClient.GeodeConnection;
import metatype.client.GeodeClient.GeodeResult;

public class ClientApp {
  public static void main(String[] args) throws Exception {
    int port = args.length == 0 ? 31415 : Integer.parseInt(args[0]);
    GeodeClient client = new NettyGeodeClient();
    GeodeConnection con = client.syncConnect("localhost", 31415, 0, TimeUnit.MILLISECONDS);
//    con.syncPut("ignore", 1L, "hi mom");
//    con.syncPut("ignore", 2L, "look no hands");
    
    System.out.println(con.<Long, String>syncGet("ignore", 1L));
    System.out.println(con.<Long, String>syncGet("ignore", 2L));
    
//    Exchanger sync = new Exchanger<>();
//    
//    client.asyncConnect("localhost", port, new GeodeCallback<GeodeClient.GeodeConnection>() {
//      @Override
//      public void onSuccess(GeodeConnection result) {
//        try {
//          sync.exchange(result);
//        } catch (InterruptedException e) {
//          Thread.currentThread().interrupt();
//        }
//      }
//      
//      @Override
//      public void onFailure(Throwable t) {
//        t.printStackTrace();
//      }
//    });
//
//    GeodeConnection con = (GeodeConnection) sync.exchange(null);
//    con.asyncPut("ignore", 1L, new byte[] { 0,  1, 2 }, new GeodeResult<Void>() {
//      @Override
//      public void onFailure(Throwable t) {
//        System.out.println("nope");
//        t.printStackTrace();
//      }
//
//      @Override
//      public void onError(int code, String msg) {
//        System.out.println(code + ":" + msg);
//      }
//
//      @Override
//      public void onSuccess(Void result) {
//        try {
//          sync.exchange(null);
//        } catch (InterruptedException e) {
//          Thread.currentThread().interrupt();
//        }
//      }
//    });
//
//    sync.exchange(null);
//    con.asyncGet("ignore", 1L, new GeodeResult<byte[]>() {
//      @Override
//      public void onFailure(Throwable t) {
//        t.printStackTrace();
//      }
//
//      @Override
//      public void onError(int code, String msg) {
//        System.out.println(code + ":" + msg);
//      }
//
//      @Override
//      public void onSuccess(byte[] result) {
//        try {
//          sync.exchange(result);
//        } catch (InterruptedException e) {
//          Thread.currentThread().interrupt();
//        }
//      }
//    });
//    
//    byte[] result = (byte[]) sync.exchange(null);
//    System.out.println(Arrays.toString(result));
//    
    con.close();
    client.close();
  }
}
