//import util.properties packages
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

//import simple producer packages
import kafka.common.KafkaException;
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.concurrent.TimeUnit;

public class producer {
    public static void main(String[] args) throws Exception{

        //Assign topicName to string variable
        final String topicName = "test6";

        // create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");

        //Set acknowledgements for producer requests.
        props.put("acks", "all");

        //If the request fails, the producer can automatically retry,
        props.put("retries", 3);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("transactional.id", "my-transactional-id");

        props.put("enable.idempotence", "true");

        props.put("group.id","groupId");

        Producer<String, String> producer = new KafkaProducer <String, String>(props);

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

        MongoCursor<Document> cursor = collection.find().iterator();

        //private final Map<TopicPartition, Long> consumedOffsets;



        producer.initTransactions();

        try {
            //producer.beginTransaction();
            while(true){

                try {
                    while (cursor.hasNext()) {
                        producer.beginTransaction();
                        //System.out.println(cursor.next().toJson());
                        String a = cursor.next().toJson();
                        final ProducerRecord<String,String> record = new ProducerRecord<String, String>(topicName, a , a);
                        System.out.println(a);
                        final RecordMetadata metadata = producer.send(record).get();
                        Map<TopicPartition, OffsetAndMetadata> groupCommit = new HashMap<TopicPartition, OffsetAndMetadata>() {
                            {
                                //put(new TopicPartition(topicName, metadata.partition()), new OffsetAndMetadata(metadata.offset(), null));
                                //put(new TopicPartition(topicName, metadata.partition()), new OffsetAndMetadata(42L,,OffsetAndMetadata.NoMetadata()));
                                put(new TopicPartition(topicName, metadata.partition()), new OffsetAndMetadata(metadata.offset(), metadata.toString()));
                            }
                        };
                        producer.sendOffsetsToTransaction(groupCommit, "groupId");
                        //producer.sendOffsetsToTransaction(groupCommit, "my-transactional-id");
                        producer.commitTransaction();

//                        try {
//                            TimeUnit.SECONDS.sleep(1);
//                            cursor.next();
//                        } catch (InterruptedException e) {
//                            e.printStackTrace();
//                        }
                    }
                } finally {
                    cursor.close();
                }
            }

        } catch (ProducerFencedException e) {
            producer.close();
        } catch (KafkaException e) {
            producer.abortTransaction();
        }
    }
}
