package com.redhat.gpte;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * CSV ingestion in a dataframe.
 * 
 * @author prakrish
 */
public class CsvToDataset {

  /**
   * main() is your entry point to the application.
   * 
   * @param args
   */
  public static void main(String[] args) {
	  CsvToDataset app = new CsvToDataset();
    app.start();
  }

  /**
   * The processing code.
   */
  private void start() {
    // Creates a session on a local master
    SparkSession spark = SparkSession.builder()
        .appName("CSV to Dataset")
       //.master("local[*]")
        //.master("spark://master:7077")
        .getOrCreate();

    // Reads a CSV file with header, called books.csv, stores it in a dataframe
    Dataset<Row> df = spark.read().format("csv")
        .option("header", "true")
        .load("src/main/resources/books.csv");

    // Shows at most 5 rows from the dataframe
    df.show();
  }
}
