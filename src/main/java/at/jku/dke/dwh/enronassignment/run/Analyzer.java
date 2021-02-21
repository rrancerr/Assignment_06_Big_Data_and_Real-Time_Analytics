package at.jku.dke.dwh.enronassignment.run;

import at.jku.dke.dwh.enronassignment.objects.Email;
import at.jku.dke.dwh.enronassignment.preparation.EmailReader;
import at.jku.dke.dwh.enronassignment.util.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static at.jku.dke.dwh.enronassignment.preparation.EmailReader.*;
import static org.apache.spark.sql.functions.*;

public class Analyzer {

    private static final String WORDS_COL_NAME = "Words";
    private static final String WORD_COUNT_COL_NAME = "WordCount";
    private static final String AVG_WORD_COUNT_COL_NAME = "avgLength";
    private static final String RECIPIENTS_COUNT_COL_NAME = "RecipientsCount";
    private static final String AVG_RECIPIENTS_COUNT_COL_NAME = "avgNoOfRecipients";

    private static final String TEMP_TABLE_NAME = "Temp_Table";

    private Analyzer() {
        throw new IllegalStateException("Utility class");
    }

    public static void task2point1() {
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
                .withColumn(RECIPIENTS_COUNT_COL_NAME, size(col(RECIPIENTS_COL_NAME)));
//                .select(ID_COL_NAME, WORD_COUNT_COL_NAME); analysis not possible with this restriction, because we still need the recipients for task d)

        dateAnalysisIdAndWords.createOrReplaceTempView(TEMP_TABLE_NAME);

        //d) Using Spark SQL, calculate the average number of recipients and the average mes-sage length of e-mails.
        Dataset<Row> averageWordCount = emailReader.getSparkSession()
                .sql("SELECT " +
                        "AVG(" + WORD_COUNT_COL_NAME + ") AS " + AVG_WORD_COUNT_COL_NAME + ", " +
                        "AVG(" + RECIPIENTS_COUNT_COL_NAME + ") AS " + AVG_RECIPIENTS_COUNT_COL_NAME + " " +
                        "FROM " + TEMP_TABLE_NAME);


        //e) Return the result as a JSON file with two fields, avgLength and avgNoOfRecipi-ents. The JSON file should be written at a specified location.
        Utils.storeToFile(Utils.convertToEmailDataset(averageWordCount), "output", Utils.JSON_FORMAT);

        emailReader.close();
    }
}
