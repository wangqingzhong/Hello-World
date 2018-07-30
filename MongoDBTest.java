package com.handbi.mongodb;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.bson.Document;

import com.mongodb.spark.MongoSpark;
import static java.util.Arrays.asList;

public class MongoDBTest {
	public static void main(String[] args) {
		System.setProperty("HADOOP_USER_NAME", "hdfs");
		
	SparkConf sc = new SparkConf()
	        .setMaster("local")
	        .setAppName("MongoSparkConnectorTour")
	        .set("spark.mongodb.input.uri",  "mongodb://192.168.11.198/zyq.t1")
	        .set("spark.mongodb.output.uri", "mongodb://192.168.11.198/zyq.t1");

	JavaSparkContext jsc = new JavaSparkContext(sc); // Create a Java Spark Context
	
	
	JavaRDD<Document> rdd=MongoSpark.load(jsc);
	
	rdd.repartition(1).saveAsTextFile("/tmp/yqzhong/t1");
	
	JavaRDD<Document> documents = jsc.parallelize(asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).map
	        (new Function<Integer, Document>() {
	        	
				private static final long serialVersionUID = -6420779045958721552L;

		public Document call(final Integer i) throws Exception {
	        return Document.parse("{test: " + i + "}");
	    }
	});
	
	MongoSpark.save(documents);
	
	
	
	
	}
	
}
