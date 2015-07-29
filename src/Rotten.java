import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Rotten {

	private static final String API_KEY = "yy5at44a4hzqqbsgnm4u47ju";
	
	private static HashMap<String, String> movieMap = new HashMap<String, String>();
	
	public static ArrayList<String> getOpeningMovies() throws IOException {
		// TODO Auto-generated method stub
		String sURL = "http://api.rottentomatoes.com/api/public/v1.0/lists/movies/opening.json?apikey=" + API_KEY; //just a string

		// Connect to the URL using java's native library
		URL url = new URL(sURL);
		HttpURLConnection request = (HttpURLConnection) url.openConnection();
		request.connect();

		// Convert to a JSON object to print data
		JsonParser jp = new JsonParser(); //from gson
		JsonElement root = jp.parse(new InputStreamReader((InputStream) request.getContent())); //Convert the input stream to a json element
		JsonObject rootobj = root.getAsJsonObject(); //May be an array, may be an object. 
		JsonArray arr = rootobj.get("movies").getAsJsonArray();
		ArrayList<String> moviesList = new ArrayList<String>();
		for (JsonElement movie : arr) {
			String id = movie.getAsJsonObject().get("id").getAsString();
			String title = movie.getAsJsonObject().get("title").getAsString();
			moviesList.add(id);
			movieMap.put(id, title);
		}

		return moviesList;
	}
	
	public static ArrayList<String> getCast(String movieID) throws IOException {
		String sURL = "http://api.rottentomatoes.com/api/public/v1.0/movies/" + movieID + "/cast.json?apikey=" + API_KEY;
		
		// Connect to the URL using java's native library
		URL url = new URL(sURL);
		HttpURLConnection request = (HttpURLConnection) url.openConnection();
		//request.addRequestProperty("User-Agent", 
				//"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)");
		request.connect();

		// Convert to a JSON object to print data
		JsonParser jp = new JsonParser(); //from gson
		JsonElement root = jp.parse(new InputStreamReader((InputStream) request.getContent())); //Convert the input stream to a json element
		JsonObject rootobj = root.getAsJsonObject(); //May be an array, may be an object. 
		JsonArray arr = rootobj.get("cast").getAsJsonArray();
		ArrayList<String> castList = new ArrayList<String>();
		for (JsonElement castMember : arr) {
			String name = castMember.getAsJsonObject().get("name").getAsString();
			castList.add(name);
		}
		
		return castList;
	}
	
	public static String getMovieTitle(String movieID) {
		return movieMap.get(movieID);
	}
}
