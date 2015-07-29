import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.AMQP.BasicProperties;
  
public class RPCServer {
  
  private static final String RPC_QUEUE_NAME = "rpc_queue";
  private static Gson gson = new Gson();
    
  public static void main(String[] argv) {
    Connection connection = null;
    Channel channel = null;
    try {
      ConnectionFactory factory = new ConnectionFactory();
      factory.setHost("localhost");
  
      connection = factory.newConnection();
      channel = connection.createChannel();
      
      channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
  
      channel.basicQos(1);
  
      QueueingConsumer consumer = new QueueingConsumer(channel);
      channel.basicConsume(RPC_QUEUE_NAME, false, consumer);
  
      System.out.println(" [x] Awaiting RPC requests");
  
      while (true) {
        String response = null;
        TimeUnit.MILLISECONDS.sleep(500); // If I had more time, I would catch the 403 errors RT throws, delay works for now
        QueueingConsumer.Delivery delivery = consumer.nextDelivery();
        
        BasicProperties props = delivery.getProperties();
        BasicProperties replyProps = new BasicProperties
                                         .Builder()
                                         .correlationId(props.getCorrelationId())
                                         .contentType("application/json")
                                         .build();
        
        try {
          String message = new String(delivery.getBody(),"UTF-8");
          JsonParser jp = new JsonParser(); //from gson
  		JsonElement root = jp.parse(message); //Convert the input stream to a json element
  		JsonObject rootobj = root.getAsJsonObject();
          String movieID = rootobj.get("id").getAsString();
          //System.out.println("got: " + message);
          ArrayList<String> castList = Rotten.getCast(movieID);
          String jsonCastList = gson.toJson(castList);
          String jsonResponse = "{\"id\":\"" + movieID + "\",\"cast_list\":" + jsonCastList + "}";
          System.out.println(" [.] get cast for movie id (" + movieID + ")");
          response = "" + jsonResponse;
        }
        catch (Exception e){
          System.out.println(" [.] " + e.toString());
          response = "";
        }
        finally {  
          channel.basicPublish( "", props.getReplyTo(), replyProps, response.getBytes("UTF-8"));
  
          channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        }
      }
    }
    catch  (Exception e) {
      e.printStackTrace();
    }
    finally {
      if (connection != null) {
        try {
          connection.close();
        }
        catch (Exception ignore) {}
      }
    }      		      
  }
}