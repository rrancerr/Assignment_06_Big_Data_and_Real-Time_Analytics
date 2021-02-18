package at.jku.dke.dwh.enronassignment.preparation;

import at.jku.dke.dwh.enronassignment.objects.Email;
import at.jku.dke.dwh.enronassignment.util.Utils;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.*;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class EmailReader {


    private static final Logger LOGGER = Logger.getLogger(EmailReader.class);
    private static final String X_BCC_IDENTIFIER = "X-bcc:";
    private static final String X_TO_IDENTIFIER = "X-To:";
    private static final String X_CC_IDENTIFIER = "X-cc:";

    private final DateFormat emailDateFormat;
    private final SparkSession sparkSession;
    private final StructType structType;
    private final List<Email> emailObjectList;

    public EmailReader() {
        // Creates a session on a local master
        this.sparkSession = SparkSession
                .builder()
                .appName("Email Reader")
                .master("local")
                .getOrCreate();

        this.structType = DataTypes.createStructType(
                new StructField[]{
                        DataTypes.createStructField(
                                "ID",
                                DataTypes.StringType,
                                false
                        ),
                        DataTypes.createStructField(
                                "Date",
                                DataTypes.TimestampType,
                                false
                        ),
                        DataTypes.createStructField(
                                "From",
                                DataTypes.StringType,
                                false
                        ),
                        DataTypes.createStructField(
                                "Recipients",
                                DataTypes.createArrayType(DataTypes.StringType),
                                false
                        ),
                        DataTypes.createStructField(
                                "Subject",
                                DataTypes.StringType,
                                true
                        ),
                        DataTypes.createStructField(
                                "Body",
                                DataTypes.StringType,
                                true
                        )
                }
        );

        this.emailObjectList = new ArrayList<>();

        this.emailDateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss Z (z)", Locale.US);
        //                                                  Fri, 26 Oct 2001 07:45:49 -0700 (PDT)
    }

    public Dataset<Email> getEmailDataset(String path) {
        LOGGER.info("Checking for files in " + path);

        ArrayList<String> pathList = (ArrayList<String>) getPathsInDirectory(path);
        LOGGER.info("Found " + pathList.size() + " files in the directory");

        //parse the files
        for (String pathInList : pathList) {
            Email newEmail = getEmailObject(pathInList);
            this.emailObjectList.add(newEmail);
        }

        //create Datasets
        ArrayList<Row> rowList = new ArrayList<>();
        for (Email email : this.emailObjectList) {
            rowList.add(RowFactory.create(email.getId(), email.getDate(), email.getFrom(), email.getRecipients(), email.getSubject(), email.getBody()));
        }

        //Dataset<Row>
        Dataset<Row> tempRow = this.sparkSession.createDataFrame(rowList, this.structType);

        //Dataset<Email>
        Dataset<Email> result = tempRow.as(Encoders.bean(Email.class));

        LOGGER.info("Datasets created!");
        return result;
    }

    private List<String> getPathsInDirectory(String path) {

        ArrayList<String> pathList = new ArrayList<>();

        File folder = new File(path);
        File[] listOfFiles = folder.listFiles();

        if (listOfFiles != null && listOfFiles.length > 0) {
            for (File file : listOfFiles) {
                if (file.isFile()) {
                    pathList.add(file.getPath());
                }
            }
        } else {
            LOGGER.error("This Path contains no files to parse: " + path);
        }

        return pathList;
    }


    private Email getEmailObject(String path) {
        LOGGER.info("Reading from this path: " + path);

        // Read the emails
        Dataset<Row> df = this.sparkSession
                .read()
                .option("wholetext", true)
                .text(path);

        String unformattedText = df.first().getString(0);
        if (unformattedText.isEmpty()) {
            LOGGER.error("Email String is empty for " + path);
            return null;
        }

        //create String-array entry for each line
        String[] strArr = unformattedText.split(System.getProperty("line.separator"));

        //get rid of unnecessary data
        ArrayList<String> rawDataLines = clearEntries(strArr);

        //merge lines that belong together
        ArrayList<String> mergedLines = mergeLines(rawDataLines);

        //remove tabs
        mergedLines = Utils.removeTabs(mergedLines);

        //map the data to an Object
        return getEmailObjectOfData(mergedLines);
    }

    /***
     * Maps the String data into an Email Object
     * @param dataLines An ArrayList of Strings containing the prepared String Data
     * @return an Email Object with the values of the ArrayList
     */
    private Email getEmailObjectOfData(ArrayList<String> dataLines) {
        String id = null;
        Timestamp date = null;
        String from = null;
        List<String> recipients = new ArrayList<>();
        String subject = null;
        StringBuilder body = new StringBuilder();

        for (String line : dataLines) {
            if (line.startsWith("Message-ID:")) {
                id = line.substring(line.indexOf(" ") + 1);
            } else if (line.startsWith("Date:")) {
                try {
                    date = new Timestamp(this.emailDateFormat.parse(line.substring(line.indexOf(" ") + 1)).getTime());
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            } else if (line.startsWith("From:")) {
                from = line.substring(line.indexOf(" ") + 1);
            } else if (line.startsWith("Subject:")) {
                subject = line.substring(line.indexOf(" ") + 1);
            } else if (line.startsWith("To:") || line.startsWith("Cc:") || line.startsWith("Bcc:") || line.startsWith(X_TO_IDENTIFIER) || line.startsWith(X_CC_IDENTIFIER) || line.startsWith(X_BCC_IDENTIFIER)) {
                //all into the recipients
                if (!line.substring(line.indexOf(" ") + 1).isEmpty()) {
                    recipients.add(line.substring(line.indexOf(" ") + 1));
                }
            } else {
                //body line
                body.append(line);
            }
        }

        return new Email(id, date, from, recipients, subject, body.toString());
    }

    /***
     * Lines that consist of multiple lines are merged into a single line.
     * @param rawDataLines The Data consisting of values that possibly go over multiple lines.
     * @return An ArrayList of Strings in the format <code>Key: Value</code>.
     */
    private static ArrayList<String> mergeLines(ArrayList<String> rawDataLines) {
        ArrayList<String> result = new ArrayList<>();
        int amountOfMultipleLines = 0;
        boolean reachedEndOfMetaData = false;
        for (int i = 0; i < rawDataLines.size(); i++) {
            String line = rawDataLines.get(i);
            if ((line.startsWith("To:") || line.startsWith("Cc:") || line.startsWith("Bcc:") || line.startsWith(X_TO_IDENTIFIER) || line.startsWith(X_CC_IDENTIFIER) || line.startsWith(X_BCC_IDENTIFIER)) && !reachedEndOfMetaData) {
                //these lines may consist of multiple lines
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(line);
                int currIdx = i + 1;
                String nextLine = rawDataLines.get(currIdx);
                while (!nextLine.startsWith("Cc:") && !nextLine.startsWith("Bcc:") && !nextLine.startsWith(X_TO_IDENTIFIER) && !nextLine.startsWith(X_CC_IDENTIFIER) && !nextLine.startsWith(X_BCC_IDENTIFIER) && !nextLine.isEmpty() && !nextLine.startsWith("Subject:")) {
                    //append the next line
                    stringBuilder.append(" ").append(nextLine);
                    //the next line also belongs to the previous
                    currIdx += 1;
                    nextLine = rawDataLines.get(currIdx);
                    amountOfMultipleLines += 1;
                }
                //got all lines that belong together -> add them to the result
                result.add(stringBuilder.toString());

                //in case the body startsWith the Meta-Data keywords, set a flag to ignore them
                if (line.startsWith(X_BCC_IDENTIFIER)) {
                    reachedEndOfMetaData = true;
                }
            } else {
                //these lines consist only of one line e.g. Message-ID, Date etc. or are part of the body
                //skip the multiple lines, because they are already added in another line
                if (amountOfMultipleLines == 0) {
                    result.add(line);
                } else {
                    amountOfMultipleLines--;
                }
            }
        }
        return result;
    }

    /***
     * Removes all unwanted information.
     * @param strArr an Array of Strings (the lines) containing the data to be parsed
     * @return an ArrayList of Strings containing only the data to be parsed
     */
    private static ArrayList<String> clearEntries(String[] strArr) {
        ArrayList<String> validLines = new ArrayList<>();
        for (String str : strArr) {
            if (!str.startsWith("Mime-Version:") && !str.startsWith("Content-Type:") && !str.startsWith("Content-Transfer-Encoding:") && !str.startsWith("X-Folder:") && !str.startsWith("X-Origin:") && !str.startsWith("X-FileName:") && !str.startsWith("X-From:")) {
                validLines.add(str);
            }
        }
        return validLines;
    }
}


