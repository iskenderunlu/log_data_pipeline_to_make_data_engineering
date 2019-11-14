import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import java.util.concurrent.TimeUnit;

public class frommongo2kafka {
    public static void main(String[] args) {
        // Prints "Hello, World" to the terminal window.
        System.out.println("Hello, World");

        // Creating a Mongo client
        MongoClient mongo = new MongoClient( "localhost" , 27017 );

        // Creating Credentials
        MongoCredential credential;
        credential = MongoCredential.createCredential("sampleUser", "myDb6",
                "password".toCharArray());
        System.out.println("Connected to the database successfully");

        // Accessing the database
        MongoDatabase database = mongo.getDatabase("myDb6");

        // Retrieving a collection
        MongoCollection<Document> collection = database.getCollection("sampleCollection");
        System.out.println("Collection sampleCollection selected successfully");

        while(true){
            // To sleep for five seconds
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //String[] brands = {"Porsche", "Mercedes Benz", "Volkswagen", "BMW", "Audi", "Land Rover", "Toyota", "Honda"};
            //countries = {"Porsche":"Germany","Mercedes Benz":"Germany","Volkswagen":"Germany","BMW":"Germany","Audi":"Germany","Land Rover":"The UK","Toyota":"Japan","Honda":"Japan"}
            HashMap<String, String> brands = new HashMap<String, String>();
            brands.put("Porsche","Germany");
            brands.put("Mercedes Benz", "Germany");
            brands.put("Volkswagen", "Germany");
            brands.put("BMW", "Germany");
            brands.put("Audi", "Germany");
            brands.put("Land Rover", "The UK");
            brands.put("Toyota", "Japan");
            brands.put("Honda", "Japan");

            Object[] crunchifyKeys = brands.keySet().toArray();
            Object brand = crunchifyKeys[new Random().nextInt(crunchifyKeys.length)];
            String country = brands.get(brand);

            DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            Date date = new Date();
            String current_date = dateFormat.format(date);

            Document document = new Document("date", current_date)
                    .append("brand", brand)
                    .append("brand_country", country)
                    .append("greetings", "Hello from "+ country);

            collection.insertOne(document);
            System.out.println("Document inserted successfully");
            System.out.println(document);

        }

    }
}



