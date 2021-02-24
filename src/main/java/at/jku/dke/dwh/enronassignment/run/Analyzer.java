package at.jku.dke.dwh.enronassignment.run;

import at.jku.dke.dwh.enronassignment.objects.Email;
import at.jku.dke.dwh.enronassignment.preparation.EmailReader;
import at.jku.dke.dwh.enronassignment.util.Utils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

import static at.jku.dke.dwh.enronassignment.preparation.EmailReader.*;
import static org.apache.spark.sql.functions.*;


/***
 * Static class for splitting up the tasks
 */
public class Analyzer {

    private static final String WORDS_COL_NAME = "Words";
    private static final String WORD_COUNT_COL_NAME = "WordCount";
    private static final String AVG_WORD_COUNT_COL_NAME = "avgLength";
    private static final String RECIPIENTS_COUNT_COL_NAME = "RecipientsCount";
    private static final String AVG_RECIPIENTS_COUNT_COL_NAME = "avgNoOfRecipients";

    private static final String EMAIL_GENERATOR_OUTPUT_DIRECTORY_PATH = "generator_output";
    private static final String TEMP_TABLE_NAME = "Temp_Table";

    private static final Logger LOGGER = Logger.getLogger(Analyzer.class);

    private Analyzer() {
        throw new IllegalStateException("Utility class");
    }

    /***
     * Demonstrates Task 2.1
     */
    public static void task2point1() {
        LOGGER.info("----------Task 2.1----------");

        EmailReader emailReader = new EmailReader();

        //a) Load a Dataset<Email> from parquet files that contain the e-mail data using the functions from the previous task
        Dataset<Email> dateAnalysis = emailReader.readFromParquetFiles(new String[]{
                "output/2021-02-21_19-29-10_output_parquet/part-00000-f27d8511-3c65-4004-a1de-4ecd02a024aa-c000.snappy.parquet",
                "output/2021-02-21_21-15-34_output_parquet/part-00000-f05b8094-d795-40dc-9f5b-1cfdc912d109-c000.snappy.parquet"
        }).orderBy(DATE_COL_NAME);

        //b) Filter the records by date so that only the e-mails in the given interval remain.
        //choose two random dates: 2000-06-23 and 2001-10-04
        dateAnalysis = dateAnalysis
                .filter(dateAnalysis.col(DATE_COL_NAME).gt(lit("2000-06-23")))
                .filter(dateAnalysis.col(DATE_COL_NAME).lt(lit("2001-10-04")));

        //c) From the filtered Dataset<Email>, create a new Dataset<Row> that represents a table with columns for the e-mail id and the length (number of words) of the e-mail. Register the data frame as a temporary table.
        Dataset<Row> dateAnalysisIdAndWords = Utils.convertToRowDataset(dateAnalysis);
        dateAnalysisIdAndWords = dateAnalysisIdAndWords
                .withColumn(WORDS_COL_NAME, split(col(BODY_COL_NAME), " "))
                .withColumn(WORDS_COL_NAME, array_remove(col(WORDS_COL_NAME), "")) // remove empty entries resulting from consecutive spaces
                .withColumn(WORD_COUNT_COL_NAME, size(col(WORDS_COL_NAME)))
                .withColumn(RECIPIENTS_COUNT_COL_NAME, size(col(RECIPIENTS_COL_NAME)))
                .select(ID_COL_NAME, WORD_COUNT_COL_NAME, RECIPIENTS_COUNT_COL_NAME); // analysis not possible with this restriction, because we still need the recipients for task d)

        dateAnalysisIdAndWords.createOrReplaceTempView(TEMP_TABLE_NAME);

        //d) Using Spark SQL, calculate the average number of recipients and the average mes-sage length of e-mails.
        Dataset<Row> averageWordCount = emailReader.getSparkSession()
                .sql("SELECT " +
                        "AVG(" + WORD_COUNT_COL_NAME + ") AS " + AVG_WORD_COUNT_COL_NAME + ", " +
                        "AVG(" + RECIPIENTS_COUNT_COL_NAME + ") AS " + AVG_RECIPIENTS_COUNT_COL_NAME + " " +
                        "FROM " + TEMP_TABLE_NAME);


        //e) Return the result as a JSON file with two fields, avgLength and avgNoOfRecipi-ents. The JSON file should be written at a specified location.
        Utils.storeAsJson(averageWordCount, "output");

        emailReader.close();
    }

    /***
     * Demonstrates Task 2.2
     */
    public static void task2point2() {
        LOGGER.info("----------Task 2.2----------");

        EmailReader emailReader = new EmailReader();

        //a) Load a Dataset<Email> from parquet files that contain the e-mail data using the functions from the previous task.
        Dataset<Email> dateAnalysis = emailReader.readFromParquetFiles(new String[]{
                "output/2021-02-21_19-29-10_output_parquet/part-00000-f27d8511-3c65-4004-a1de-4ecd02a024aa-c000.snappy.parquet",
                "output/2021-02-21_21-15-34_output_parquet/part-00000-f05b8094-d795-40dc-9f5b-1cfdc912d109-c000.snappy.parquet",
                "output/2021-02-21_23-57-00_output_parquet/part-00000-ce0eed3a-d6cf-4005-8cc0-86883fc7f759-c000.snappy.parquet",
                "output/2021-02-21_23-57-26_output_parquet/part-00000-b2322a1f-8de5-4b8a-8e9e-bf4ff21e5cda-c000.snappy.parquet"
        }).orderBy(DATE_COL_NAME);

        //b) Filter the records by date so that only the e-mails in the given interval remain.
        //choose two random dates: 2000-06-23 and 2001-10-04
        dateAnalysis = dateAnalysis
                .filter(dateAnalysis.col(DATE_COL_NAME).gt(lit("2000-06-23")))
                .filter(dateAnalysis.col(DATE_COL_NAME).lt(lit("2001-10-04")));


        //c) From the filtered Dataset<Email>, create a new Dataset<Row> for a table with columns for the e-mail id, the sender (From), the length (number of words) of the e-mail, and the number of recipients. Register the data frame as a temporary table.
        Dataset<Row> dateAnalysisIdAndWords = Utils.convertToRowDataset(dateAnalysis);
        dateAnalysisIdAndWords = dateAnalysisIdAndWords
                .withColumn(WORDS_COL_NAME, split(col(BODY_COL_NAME), " "))
                .withColumn(WORDS_COL_NAME, array_remove(col(WORDS_COL_NAME), "")) // remove empty entries resulting from consecutive spaces
                .withColumn(WORD_COUNT_COL_NAME, size(col(WORDS_COL_NAME)))
                .withColumn(RECIPIENTS_COUNT_COL_NAME, size(col(RECIPIENTS_COL_NAME)))
                .select(ID_COL_NAME, FROM_COL_NAME, WORD_COUNT_COL_NAME, RECIPIENTS_COUNT_COL_NAME);

        dateAnalysisIdAndWords.createOrReplaceTempView(TEMP_TABLE_NAME);

        //d) Using Spark SQL, calculate the average number of recipients of an e-mail for each sender as well as the average length of an e-mail for each sender.
        Dataset<Row> averageWordCount = emailReader.getSparkSession()
                .sql("SELECT " +
                        FROM_COL_NAME + " AS sender, " +
                        "AVG(" + WORD_COUNT_COL_NAME + ") AS " + AVG_WORD_COUNT_COL_NAME + ", " +
                        "AVG(" + RECIPIENTS_COUNT_COL_NAME + ") AS " + AVG_RECIPIENTS_COUNT_COL_NAME + " " +
                        "FROM " + TEMP_TABLE_NAME + " " +
                        "GROUP BY " + FROM_COL_NAME
                );

        //e) Return the result as a JSON file with fields for the sender, avgNoOfRecpients, and avgLength. The JSON file should be written at a specified location.
        Utils.storeAsJson(averageWordCount, "output");

        emailReader.close();
    }

    /***
     * Demonstrates Task 4.1
     */
    public static void task4point1() {
        LOGGER.info("----------Task 4.1----------");

        EmailReader emailReader = new EmailReader();

        Dataset<Row> df = emailReader.getSparkSession()
                .readStream()
                .format("text")
                .load(EMAIL_GENERATOR_OUTPUT_DIRECTORY_PATH);

        try {
            df.writeStream()
                    .outputMode(OutputMode.Update())
                    .format("console")
                    .option("truncate", "true")
                    .start().awaitTermination();
        } catch (TimeoutException | StreamingQueryException e) {
            e.printStackTrace();
        }

        emailReader.close();
    }
}
