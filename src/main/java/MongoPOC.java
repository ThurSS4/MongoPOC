import com.mongodb.MongoTimeoutException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class MongoPOC {

    public static void main(String[] args) {
        MongoConnector connector = new MongoConnector();
        try {
//            MongoDatabase database = connector.getDatabase("testdb");
//            MongoCollection<Document> collection = database.getCollection("testing");
//            if (collection.countDocuments() == 0) {
//                connector.addDummyDataTo(collection);
//            } else {
//                connector.printDocumentsFor(collection);
//            }

            SparkConnector sparkConnect = new SparkConnector();
            sparkConnect.buildAndWrite();
        } catch (MongoTimeoutException mte) {
            System.out.println("The connection could not be made due to a time out. Please check the server address!");
        } catch (Exception e) {
            System.out.println("An exception was thrown: " + e);
        }
    }
}
