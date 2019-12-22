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

import static java.util.Collections.singletonList;
import static java.util.Arrays.asList;

public class SparkConnector implements Serializable {
    private transient SparkSession spark;
    private transient JavaSparkContext jsc;

    public SparkConnector() {}

    public void buildAndWrite() {
//        spark = SparkSession.builder()
//                .master("local")
//                .appName("MongoPOC")
//                .config("spark.mongodb.input.uri", "mongodb://192.168.2.103/testdb.testing")
//                .config("spark.mongodb.output.uri", "mongodb://192.168.2.103/testdb.testing")
//                .getOrCreate();
//
//        jsc = new JavaSparkContext(spark.sparkContext());
//        writeToMongoDB();
//        readFromMongoDB();
//        readFromMongoDBWithAggregation();
        readFromMongoDBWithSQL();
        jsc.close();
    }

    private void writeToMongoDB() {
        System.out.println("\nStarting to write RDD's to MongoDB...");
        JavaRDD<Document> documents = jsc.parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 1).map(
                new Function<Integer, Document>() {
                    public Document call(final Integer i) {
                        return Document.parse("{phrase: " + i + "}");
                    }
                }
        );

        MongoSpark.save(documents);
        System.out.println("Done writing RDD's to MongoDB.");
    }

    private void readFromMongoDB() {
        System.out.println("\nStarting to read RDD's from MongoDB...");
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
        System.out.println("There are " + rdd.count() + " rdd's found.");
        System.out.println("The first rdd looks like this: " + rdd.first().toJson());
        System.out.println("Done reading RDD's from MongoDB.");
    }

    private void readFromMongoDBWithAggregation() {
        // Load and analyze data from MongoDB
        JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);

        // Use aggregation to filter a RDD
        JavaMongoRDD<Document> aggregatedRdd = rdd.withPipeline(
                singletonList(
                        Document.parse("{ $match: { phrase: { $gt: 5 } } }")
                )
        );

        // Analyze data from MongoDB
        System.out.println("Amount of records with field phrase greater than 5: " + aggregatedRdd.count());
        System.out.println("The first rdd looks like this: " + aggregatedRdd.first().toJson());
    }

    private void readFromMongoDBWithSQL() {
        spark = SparkSession.builder()
                .master("local")
                .appName("MongoPOC")
                .config("spark.mongodb.input.uri", "mongodb://192.168.2.103/testdb.characters")
                .config("spark.mongodb.output.uri", "mongodb://192.168.2.103/testdb.characters")
                .getOrCreate();

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
        Dataset<Row> centenarians = spark.sql("SELECT name, age FROM characters WHERE age >= 100 ORDER BY age desc");
        centenarians.show();

        // Write the data to the "hundredClub" collection
        MongoSpark.write(centenarians).option("collection", "hundredClub").mode("overwrite").save();

        // Load the data from the "hundredClub" collection
        MongoSpark.load(spark, ReadConfig.create(spark).withOption("collection", "hundredClub"), Character.class).show();
    }
}
