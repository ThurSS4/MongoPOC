import com.mongodb.Block;
import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.*;
import com.mongodb.connection.ClusterSettings;
import org.bson.Document;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.client.model.Filters.*;

public class MongoConnector implements Serializable {
    private MongoClient mongoClient;
    private final String HOST_ADDRESS = "localhost";
    private final int HOST_PORT = 27017;

    /**
     * In the constructor of this class the MongoClient is build and connected with our MongoDB instance for which
     * we need to provide it's location and the port to use.
     */
    public MongoConnector() {
        System.out.println("Connecting to MongoDB...");
        mongoClient = MongoClients.create(
                MongoClientSettings.builder()
                        .applyToClusterSettings(new Block<ClusterSettings.Builder>() {
                            public void apply(ClusterSettings.Builder builder) {
                                builder.hosts(Collections.singletonList(new ServerAddress(HOST_ADDRESS, HOST_PORT)));
                            }
                        })
                        .build());
    }

    /**
     * getDatabase connects to MongoDB instance to get the desired database and returns it to the caller
     * @param databaseName the name of the database you wish to get
     * @return the requested MongoDB database
     */
    public MongoDatabase getDatabase(String databaseName) {
        return mongoClient.getDatabase(databaseName);
    }

    /**
     * Prints all documents that are found in the given collection to the console if there are any to print.
     * The commented out part is only to be used with the dummy data as it refers to a field in that data.
     * @param collection the collection of which the documents should be printed
     */
    public void printDocumentsFor(MongoCollection<Document> collection) {
        long amountOfDocs = collection.countDocuments();
        if (amountOfDocs > 0) {
            System.out.println("There are " + amountOfDocs + " documents in this collection:");

            // Print phrase from each document in the collection (only to use with dummy data)
//            FindIterable<Document> documents = collection.find();
//            for (Document doc : documents) {
//                System.out.println(doc.get("phrase"));
//            }

            // Iterate through the collection and print the complete document for all documents in it
            MongoCursor<Document> cursor = collection.find().iterator();
            try {
                while (cursor.hasNext()) {
                    System.out.println(cursor.next().toJson());
                }
            } finally {
                cursor.close();
                System.out.println("Connection closed...");
            }
        } else {
            System.out.println("No documents found in this collection.");
        }
    }

    /**
     * Create dummy data to add to a given collection. The dummy will always be added whether or not the given
     * collection already contains data.
     * @param collection the collection to which the dummy data should be added
     */
    public void addDummyDataTo(MongoCollection<Document> collection) {
        System.out.println("\nAdding dummy data to the collection.");
        // Creating some documents with one or two fields
        Document noTypeDoc = new Document("phrase", "Better test twice than fail once");
        Document testDoc = new Document("phrase", "When you test, you are the best")
                .append("type", "test phrase");
        Document catchyDoc = new Document("phrase", "Wanna be the best? Write a good test!")
                .append("type", "catchy phrase");

        // Create a list of documents and add above document to it
        List<Document> dummyDocs = new ArrayList<Document>();
        dummyDocs.add(noTypeDoc);
        dummyDocs.add(testDoc);
        dummyDocs.add(catchyDoc);

        // Insert the documents into the given collection and print its contents to the console
        collection.insertMany(dummyDocs);
        printDocumentsFor(collection);

        // Get the first document from the collection and updates it's type field. If the field doesn't exist on
        // the document then it will be added to it. Afterwards the complete collection will be printed to the console
        System.out.println("\nUpdating the type of the first document.");
        Document firstDoc = collection.find().first();
        if (firstDoc != null) {
            collection.updateOne(firstDoc, new Document("$set", new Document("type", "main phrase")));
        }
        printDocumentsFor(collection);

        // Delete one document from the collection which has 'catchy phrase' as value for its type field and print
        // the entire collection
        System.out.println("\nRemoving document of type catchy phrase");
        collection.deleteOne(eq("type", "catchy phrase"));
        System.out.println("Print the updated collection:");
        printDocumentsFor(collection);

        // Create and insert a new document into the collection. After that print the updated collection
        System.out.println("\nAdding new document to the collection.");
        Document doc = new Document("phrase", "Wanna be the best? Write a good test!")
                .append("type", "catchy phrase");
        collection.insertOne(doc);
        System.out.println("Print the updated collection:");
        printDocumentsFor(collection);

        // Delete all documents from the collection when equal to or greater then the given values for the given fields
        System.out.println("Removing all documents from the collection: ");
        collection.deleteMany(gte("phrase", "A"));
        collection.deleteMany(gte("type", "a"));
        collection.deleteMany(gte("phrase", 1));

        // Print the amount of documents in the collection
        System.out.println("The collection now has " + collection.countDocuments() + " documents.");
    }
}
