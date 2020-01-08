import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import java.io.Serializable;
import java.util.Arrays;

import static java.util.Collections.singletonList;
import static java.util.Arrays.asList;

public class SparkConnector implements Serializable {
    private transient SparkSession spark;
    private transient JavaSparkContext jsc;
    private final String hostAddress = "localhost";

    public SparkConnector() {}

    public void buildAndExecuteTasks() {
        // Setup the SparkSession, master local[2] means two cores can be used and Spark is running locally
        // newdb is the database name, sparktest the collection
        spark = SparkSession.builder()
                .master("local[2]")
                .config("spark.mongodb.input.uri", "mongodb://" + hostAddress + "/newdb.sparktest")
                .config("spark.mongodb.output.uri", "mongodb://" + hostAddress + "/newdb.sparktest")
                .getOrCreate();

        // Create a new JavaSparkContext
        jsc = new JavaSparkContext(spark.sparkContext());

        // Example 1 - Write to and read from database
        writeToMongoDB();
        readFromMongoDB();

        // Example 2 - Read from the database with aggregation, can be used in combination with Example 1
        readFromMongoDBWithAggregation();

        // Example 3 - Read from the database using SQL, it's best to comment out ALL of ABOVE
        readFromMongoDBWithSQL();

        // Closing the context when we're done
        jsc.close();
    }

    /**
     * writeToMongoDB  creates a JavaRDD with 10 documents containing the field name 'number' and an actual
     * number as it's value. By calling save the documents are saved in the output.uri location of the session.
     */
    private void writeToMongoDB() {
        System.out.println("\nStarting to write RDD's to MongoDB...");
        JavaRDD<Document> documents = jsc.parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 1).map(
                new Function<Integer, Document>() {
                    public Document call(final Integer i) {
                        return Document.parse("{number: " + i + "}");
                    }
                }
        );

        MongoSpark.save(documents);
        System.out.println("Done writing RDD's to MongoDB.");
    }

    /**
     * readFromMongoDB connects to the input.uri of the session, loads all the documents into a JavaMongoRDD and
     * prints the amount of documents as well as an example of the first document to the console.
     */
    private void readFromMongoDB() {
        System.out.println("\nStarting to read RDD's from MongoDB...");
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);

        System.out.println("There are " + rdd.count() + " rdd's found.");
        System.out.println("The first rdd looks like this: " + rdd.first().toJson());
        System.out.println("Done reading RDD's from MongoDB.");
    }

    /**
     * readFromMongoDBWithAggregation connects to the input.uri of the session, first loads all documents into a
     * JavaMongoRDD and then aggregates on that to create a new JavaMongoRDD with just the desired data. In it's
     * current state it has a pipeline with 3 aggregation statements. To use just one aggregation comment that part
     * out and use the singletonList example. After either one the amount of documents and an example of the first
     * document are printed to the console.
     */
    private void readFromMongoDBWithAggregation() {
        // Load and analyze data from MongoDB
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);

        // Use aggregation to filter a RDD
        JavaMongoRDD<Document> aggregatedRdd = rdd.withPipeline(
                Arrays.asList(
                        Document.parse("{ $match: { number: { $gt: 5 } } }"),
                        Document.parse("{ $match: { number: { $lte: 8 } } }"),
                        Document.parse("{ $sort: { number: -1 } }")
                )
//                singletonList(
//                        Document.parse("{ $match: { number: { $gt: 5 } } }")
//                )
        );

        // Analyze data from MongoDB
        System.out.println("Amount of records with field phrase greater than 5: " + aggregatedRdd.count());
        System.out.println("The first rdd looks like this: " + aggregatedRdd.first().toJson());
    }

    /**
     * readFromMongoDBWithSQL uses a different session as the other examples. Please comment out ALL code ABOVE the
     * call to this function in the buildAndExecuteTasks for it to work properly!
     * This method also needs the Character class as a model in order to work properly.
     *
     * After setting up the Session and Context and loading the data this method uses an SQL call to create the dataset.
     * Then it creates a new collection on the database if necessary which it loads with the results from the query.
     * While running the process is printed to the console.
     */
    private void readFromMongoDBWithSQL() {
        // Setup the SparkSession, master local[2] means two cores can be used and Spark is running locally
        // newdb is the database name, sparktest the collection
        spark = SparkSession.builder()
                .master("local[2]")
                .config("spark.mongodb.input.uri", "mongodb://" + hostAddress + "/newdb.characters")
                .config("spark.mongodb.output.uri", "mongodb://" + hostAddress + "/newdb.characters")
                .getOrCreate();

        // Create a new JavaSparkContext
        jsc = new JavaSparkContext(spark.sparkContext());

        // Load data and infer schema, disregard toDF() name as it returns Dataset
        Dataset<Row> implicitDS = MongoSpark.load(jsc).toDF();
        implicitDS.printSchema();
        implicitDS.show();

        // Load data with explicit schema
        Dataset<Character> explicitDS = MongoSpark.load(jsc).toDS(Character.class);
        explicitDS.printSchema();
        explicitDS.show();

        // Create the temp view and execute the query
        explicitDS.createOrReplaceTempView("characters");
        Dataset<Row> centenarians = spark.sql("SELECT name, age FROM characters WHERE age >= 100 ORDER BY age DESC");
        centenarians.show();

        // Write the data to the "hundredClub" collection
        MongoSpark.write(centenarians).option("collection", "hundredClub").mode("overwrite").save();

        // Load the data from the "hundredClub" collection
        MongoSpark.load(spark, ReadConfig.create(spark).withOption("collection", "hundredClub"), Character.class).show();
    }
}
