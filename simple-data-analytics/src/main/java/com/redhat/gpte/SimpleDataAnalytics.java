package com.redhat.gpte;
import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SimpleDataAnalytics {

	public static void main(String[] args) throws AnalysisException {

		SparkSession spark = SparkSession.builder().appName("documentation").master("local").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");
		List<Row> list=new ArrayList<Row>();
		list.add(RowFactory.create("one"));
		list.add(RowFactory.create("two"));
		list.add(RowFactory.create("three"));
		list.add(RowFactory.create("four"));

		List<org.apache.spark.sql.types.StructField> listOfStructField=new ArrayList<org.apache.spark.sql.types.StructField>();
		listOfStructField.add(DataTypes.createStructField("test", DataTypes.StringType, true));
		StructType structType=DataTypes.createStructType(listOfStructField);
		Dataset<Row> data=spark.createDataFrame(list,structType);
		data.show();

		//Lets create the dataset of row using the Arrays asList Function

		Dataset<Row> test= spark.createDataFrame(Arrays.asList(
				new Movie("movie1",2323d,"1212"),
				new Movie("movie2",2323d,"1212"),
				new Movie("movie3",2323d,"1212"),
				new Movie("movie4",2323d,"1212")
				), Movie.class);

		test.registerTempTable("Movie");
	    test.select(col("name")).show();
	    spark.sql("SELECT * FROM Movie");

            test.show();	    
	}
}