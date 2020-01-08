import com.mongodb.MongoTimeoutException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class MongoPOC {

    public static void main(String[] args) {
        runMongoAndSpark();
    }

    private static void runMongoAndSpark() {
        // Create a new MongoConnector
        MongoConnector connector = new MongoConnector();

        try {
            // Try to get the database and collection
            MongoDatabase database = connector.getDatabase("newdb");
            MongoCollection<Document> collection = database.getCollection("ourCol");

            // CHeck if there are any documents in the collection
            if (collection.countDocuments() == 0) {
                // If not we'll first add dummy data to the collection
                connector.addDummyDataTo(collection);
            } else {
                // If there are documents we print them to the console
                connector.printDocumentsFor(collection);
            }

            // Create a new SparkConnector and execute the buildAndExecuteTasks method
            SparkConnector sparkConnect = new SparkConnector();
            sparkConnect.buildAndExecuteTasks();
        } catch (MongoTimeoutException mte) {
            // The connection to the MongoDB timed out
            System.out.println("The connection could not be made due to a time out. Please check the server address!");
        } catch (Exception e) {
            // Some other exception was thrown
            System.out.println("An exception was thrown: " + e);
        }
    }
}
