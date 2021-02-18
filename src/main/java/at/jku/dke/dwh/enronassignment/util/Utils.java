package at.jku.dke.dwh.enronassignment.util;

import at.jku.dke.dwh.enronassignment.objects.Email;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static at.jku.dke.dwh.enronassignment.preparation.EmailReader.*;

public class Utils {

    public static final String PARQUET_STRING = "parquet";

    private static final Logger LOGGER = Logger.getLogger(Utils.class);
    private static final String FILE_DATE_PREFIX_DATE_FORMAT = "yyyy-MM-dd_HH-mm-ss";

    private Utils() {
        throw new IllegalStateException("Utility class");
    }

    /***
     * Prints out all entries of an ArrayList to the console
     * @param arrayList the ArrayList in which the entries shall get printed out
     */
    public static void printArrayList(List<String> arrayList) {
        if (arrayList.isEmpty()) {
            LOGGER.error("ArrayList is empty!");
            return;
        }
        for (String entry : arrayList) {
            LOGGER.debug(entry);
        }
    }

    /***
     * Removes tabs and double-whitespaces of a List of Strings
     * @param list the list in which the Strings get parsed
     * @return an ArrayList of Strings without the double-whitespaces and tabs
     */
    public static List<String> removeTabs(List<String> list) {
        ArrayList<String> result = new ArrayList<>();
        for (String str : list) {
            if (str.contains("To:") || str.contains("Cc:") || str.contains("Bcc:") || str.contains(X_TO_IDENTIFIER) || str.contains(X_CC_IDENTIFIER) || str.contains(X_BCC_IDENTIFIER) || str.contains("Subject")) {
                //replace tabs and double-spaces
                result.add(str.replace("\t", "").replace("  ", " "));
            } else {
                result.add(str);
            }
        }
        return result;
    }

    /***
     * Converts a <code>Dataset&lt;Email&gt;</code> to a <code>Dataset&lt;Row&gt;</code>
     * @param pEmailDataset the Email Dataset that shall be converted
     * @return a <code>Dataset&lt;Row&gt;</code> of the given <code>Dataset&lt;Email&gt;</code>
     */
    public static Dataset<Row> convertToRowDataset(Dataset<Email> pEmailDataset) {
        return pEmailDataset.as(Encoders.bean(Row.class));
    }

    /***
     * Converts a <code>Dataset&lt;Row&gt;</code> to a <code>Dataset&lt;Email&gt;</code>
     * @param pRowDataset the Row Dataset that shall be converted
     * @return a <code>Dataset&lt;Email&gt;</code> of the given <code>Dataset&lt;Row&gt;</code>
     */
    public static Dataset<Email> convertToEmailDataset(Dataset<Row> pRowDataset) {
        return pRowDataset.as(Encoders.bean(Email.class));
    }

    /***
     * Appends the second dataframe to the first
     * @param firstDf a <code>Dataset&lt;Email&gt;</code>
     * @param secDf a <code>Dataset&lt;Email&gt;</code>
     * @return a <code>Dataset&lt;Email&gt;</code> in which the second Dataframe is appended to the first
     */
    public static Dataset<Email> concatenateDatasets(Dataset<Email> firstDf, Dataset<Email> secDf) {
        return firstDf.unionAll(secDf);
    }

    /***
     * Stores a Dataframe as parquet file in a sepcific path.
     * @param emailDataset the Dataframe that shall be stored into a parquet file
     * @param path the path as a String where the parquet file shall be saved, no File-Separator charactor at the end required!
     */
    public static void storeAsParquet(Dataset<Email> emailDataset, String path) {
        //get current time
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern(FILE_DATE_PREFIX_DATE_FORMAT);
        LocalDateTime now = LocalDateTime.now();

        //build a unique filename
        String parquetFilePath = path + File.separator + dtf.format(now) + "_output_parquet";

        //save it to path
        emailDataset
                .write()
                .format(PARQUET_STRING)
                .save(parquetFilePath);

    }
}
