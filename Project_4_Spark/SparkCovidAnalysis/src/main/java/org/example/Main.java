package org.example;


import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class Main {

    private static final boolean useCache=true;
    public static void main(String[] args) {

        final String master = args.length > 0 ? args[0] : "local[1]";
        final String filePath = args.length > 1 ? args[1] : "./files/data.csv";
        //final String filePath = args.length > 1 ? args[1] : "~/Documents/SPARK/data.csv";
        final SparkSession spark = SparkSession
                .builder()
                .master(master)
                .appName("Covid-19")
                //.config("spark.sql.shuffle.partitions", 1)
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        final List<StructField> mSchemaFields = new ArrayList<>();
        mSchemaFields.add(DataTypes.createStructField("dateRep", DataTypes.DateType, true));
        mSchemaFields.add(DataTypes.createStructField("day", DataTypes.IntegerType, true));
        mSchemaFields.add(DataTypes.createStructField("month", DataTypes.IntegerType, true));
        mSchemaFields.add(DataTypes.createStructField("year", DataTypes.IntegerType, true));
        mSchemaFields.add(DataTypes.createStructField("cases", DataTypes.IntegerType, true));
        mSchemaFields.add(DataTypes.createStructField("deaths", DataTypes.IntegerType, true));
        mSchemaFields.add(DataTypes.createStructField("countriesAndTerritories", DataTypes.StringType, true));
        mSchemaFields.add(DataTypes.createStructField("geoId", DataTypes.StringType, true));
        mSchemaFields.add(DataTypes.createStructField("countryTerritoryCode", DataTypes.StringType, true));
        mSchemaFields.add(DataTypes.createStructField("popData2019", DataTypes.IntegerType, true));
        mSchemaFields.add(DataTypes.createStructField("continentExp", DataTypes.StringType, true));
        final StructType mSchema = DataTypes.createStructType(mSchemaFields);

        //long startTime = System.nanoTime();

        final Dataset<Row> covidData = spark
                .read()
                .option("header", "true")
                .option("delimiter", ",")
                .option("dateFormat", "dd/MM/yyyy")
                .schema(mSchema)
                .csv(filePath);


        // Q1.  Seven days moving average of new reported cases, for each country and for each da
        WindowSpec window=Window.partitionBy("countriesAndTerritories").orderBy("dateRep").rowsBetween(-6,0);
        Dataset<Row> query1= covidData.select(col("dateRep"),col("countriesAndTerritories"), avg("cases").over(window)
                        .cast(new DecimalType(20,4)).as("AVG"))
                .orderBy(desc("dateRep"));



        query1.show();
        query1.cache();

        //Q2. Percentage increase (with respect to the day before) of the seven days moving average, for each country
        //and for each day

        WindowSpec window2=Window.partitionBy("countriesAndTerritories").orderBy("dateRep");

        Dataset<Row> query2=query1.withColumn("DayBeforeAVG",lag(col("AVG"),1).
                over(window2));
        query2= query2.withColumn("percentageIncreased",when(col("dayBeforeAVG").equalTo(0), "0")
                .otherwise(col("AVG").minus(col("DayBeforeAVG"))
                        .divide(col("DayBeforeAVG"))
                        .cast(new DecimalType(20,4))))
                .orderBy(desc("dateRep"),desc("percentageIncreased"));

        query2.show();
        query2.cache();


        //Q3. Top 10 countries with the highest percentage increase of the seven days moving average, for each day
        WindowSpec window3=Window.partitionBy("dateRep").orderBy(desc("percentageIncreased"));
        Dataset<Row> query3=query2.select(col("dateRep"),col("countriesAndTerritories"),col("percentageIncreased"),
                rank().over(window3).as("rank")).select(col("dateRep"),col("countriesAndTerritories"),col("percentageIncreased"),col("rank"))
                        .where(col("rank").lt(11)).orderBy(desc("dateRep"),col("rank"));
        query3.show();


    }
}