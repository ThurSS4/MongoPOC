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

    public MongoConnector() {
        System.out.println("Connecting to MongoDB...");
        mongoClient = MongoClients.create(
                MongoClientSettings.builder()
                        .applyToClusterSettings(new Block<ClusterSettings.Builder>() {
                            public void apply(ClusterSettings.Builder builder) {
                                builder.hosts(Collections.singletonList(new ServerAddress("192.168.2.103", 27017)));
                            }
                        })
                        .build());
    }

    public MongoDatabase getDatabase(String databaseName) {
        return mongoClient.getDatabase(databaseName);
    }

    public void printDocumentsFor(MongoCollection<Document> collection) {
        long amountOfDocs = collection.countDocuments();
        if (amountOfDocs > 0) {
            System.out.println("There are " + amountOfDocs + " documents in this collection:");

            // Print phrase from each document in the collection
//            FindIterable<Document> documents = collection.find();
//            for (Document doc : documents) {
//                System.out.println(doc.get("phrase"));
//            }

            // Print the complete document for all documents in the collection
            MongoCursor<Document> cursor = collection.find().iterator();
            try {
                while (cursor.hasNext()) {
                    System.out.println(cursor.next().toJson());
                }
            } finally {
                cursor.close();
            }
        } else {
            System.out.println("No documents found in this collection.");
        }
    }

    public void addDummyDataTo(MongoCollection<Document> collection) {
        System.out.println("\nAdding dummy data to the collection.");
        Document noTypeDoc = new Document("phrase", "Better test twice than fail once");
        Document testDoc = new Document("phrase", "When you test, you are the best")
                .append("type", "test phrase");
        Document catchyDoc = new Document("phrase", "Wanna be the best? Write a good test!")
                .append("type", "catchy phrase");

        List<Document> dummyDocs = new ArrayList<Document>();
        dummyDocs.add(noTypeDoc);
        dummyDocs.add(testDoc);
        dummyDocs.add(catchyDoc);
        collection.insertMany(dummyDocs);
        printDocumentsFor(collection);

        System.out.println("\nUpdating the type of the first document.");
        Document firstDoc = collection.find().first();
        if (firstDoc != null) {
            collection.updateOne(firstDoc, new Document("$set", new Document("type", "main phrase")));
        }
        printDocumentsFor(collection);

        System.out.println("\nRemoving document of type catchy phrase");
        collection.deleteOne(eq("type", "catchy phrase"));

        System.out.println("Print the updated collection:");
        printDocumentsFor(collection);

        System.out.println("\nAdding new document to the collection.");
        Document doc = new Document("phrase", "Wanna be the best? Write a good test!")
                .append("type", "catchy phrase");
        collection.insertOne(doc);

        System.out.println("Print the updated collection:");
        printDocumentsFor(collection);

        System.out.println("Removing all documents from the collection: ");
        collection.deleteMany(gte("phrase", "A"));
        collection.deleteMany(gte("type", "a"));
        collection.deleteMany(gte("phrase", 1));

        System.out.println("The collection now has " + collection.countDocuments() + " documents.");
    }
}
