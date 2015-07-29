import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.ShutdownSignalException;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;


public class RPCClientAsync {

	private Connection connection;
	private Channel sendChannel;
	private Channel receiveChannel;
	private String requestQueueName = "rpc_queue";
	private String replyQueueName;
	private QueueingConsumer consumer;
	private static Gson gson = new Gson(); 
	private static ConcurrentLinkedQueue<String> threadSafeQueue = new ConcurrentLinkedQueue<String>();

	public RPCClientAsync() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		connection = factory.newConnection();
		sendChannel = connection.createChannel();
		receiveChannel = connection.createChannel();

		replyQueueName = receiveChannel.queueDeclare().getQueue();
		consumer = new QueueingConsumer(receiveChannel);
		receiveChannel.basicConsume(replyQueueName, true, consumer);
		
		Thread t = new Thread( new Runnable() {
			public void run() {
				while (true) {
					QueueingConsumer.Delivery delivery = null;
					try {
						delivery = consumer.nextDelivery();
					} catch (ShutdownSignalException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (ConsumerCancelledException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					String response = null;
					try {
						response = new String(delivery.getBody(),"UTF-8");
					} catch (UnsupportedEncodingException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					//System.out.println(response);
					JsonParser jp = new JsonParser(); //from gson
					JsonElement root = jp.parse(response); //Convert the input stream to a json element
					JsonObject rootobj = root.getAsJsonObject(); //May be an array, may be an object. 
					String movieID = rootobj.get("id").getAsString();
					JsonArray castList = rootobj.get("cast_list").getAsJsonArray();
					
					
					PrintWriter writer = null;
					try {
						writer = new PrintWriter("./"+movieID+".txt", "UTF-8");
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (UnsupportedEncodingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					//PrintWriter writer = new PrintWriter("./movieLogs/"+movieID+".txt", "UTF-8");
					System.out.println(" [.] Got '" + response + "'");
					for (JsonElement castMember : castList) {
						writer.println(castMember.getAsString());
					}
					
					writer.close();
					
					threadSafeQueue.remove(movieID);
				}
				

			}
		});
		
		t.start();
		

	}

	public void call(String message) throws Exception {



		String response = null;
		String corrId = UUID.randomUUID().toString();

		BasicProperties props = new BasicProperties
				.Builder()
		.correlationId(corrId)
		.contentType("application/json")
		.replyTo(replyQueueName)
		.build();

		sendChannel.basicPublish("", requestQueueName, props, message.getBytes("UTF-8"));

	}

	public void close() throws Exception {
		connection.close();
	}

	public static void main(String[] argv) throws IOException {



		RPCClientAsync movieRpc = null;
		String response = null;
		ArrayList<String> openingMovies = Rotten.getOpeningMovies();

		try {
			movieRpc = new RPCClientAsync();
			
			for (String movieID : openingMovies) {


				String jsonMessage  = "{\"id\":\""+movieID+"\"}";

				System.out.println(" [x] Requesting cast for movie (" + Rotten.getMovieTitle(movieID) + ")");
				System.out.println(" [x] Sent: "+ jsonMessage);
				movieRpc.call(jsonMessage);
				threadSafeQueue.add(movieID);
			}
			
			while(true){}
		}
		catch  (Exception e) {
			e.printStackTrace();
		}
		finally {
			if (movieRpc!= null) {
				try {
					movieRpc.close();
				}
				catch (Exception ignore) {}
			}
		}

	}

}

