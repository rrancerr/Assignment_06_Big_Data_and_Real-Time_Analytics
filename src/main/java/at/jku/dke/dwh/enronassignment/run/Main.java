package at.jku.dke.dwh.enronassignment.run;

import at.jku.dke.dwh.enronassignment.objects.Email;
import at.jku.dke.dwh.enronassignment.preparation.EmailReader;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;

import static at.jku.dke.dwh.enronassignment.preparation.EmailReader.*;

/***
 * Main class - to run the program.
 *
 * @author Marco Stadler
 */


public class Main {
    private static final Logger LOGGER = Logger.getLogger(Main.class);

    public static void main(String[] args) {
        EmailReader emailReader = new EmailReader();

        String[] paths = {
                "output/2021-02-18_22-52-05_output.parquet/part-00000-7044b63d-4023-4a7c-b3df-7cd479a34945-c000.snappy.parquet",
                "output/2021-02-18_22-58-18_output.parquet/part-00000-90ef80c3-055a-42b1-a2b3-d199adabf9e5-c000.snappy.parquet",
                "output/2021-02-18_23-00-20_output.parquet/part-00000-85a73358-2969-43b6-a65d-941a46d2c7d8-c000.snappy.parquet",
                "output/2021-02-18_23-04-32_output.parquet/part-00000-fd059c05-e759-413a-a760-ca35ce8ce8ef-c000.snappy.parquet",
                "output/2021-02-18_23-05-59_output_parquet/part-00000-2bb2d4f2-5b77-4840-989f-f931a760bd97-c000.snappy.parquet"
        };

        Dataset<Email> temp = emailReader.readFromParquetFiles(paths);
        temp.printSchema();
        temp.select(
                ID_COL_NAME,
                DATE_COL_NAME,
                FROM_COL_NAME,
                RECIPIENTS_COL_NAME,
                SUBJECT_COL_NAME
        ).show(100);

        emailReader.close();
    }
}
