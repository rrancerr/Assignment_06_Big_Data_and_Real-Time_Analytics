package at.jku.dke.dwh.enronassignment.run;

import at.jku.dke.dwh.enronassignment.objects.Email;
import at.jku.dke.dwh.enronassignment.preparation.EmailReader;
import org.apache.spark.sql.Dataset;

/***
 * Main class - to run the program.
 *
 * @author Marco Stadler
 */

public class Main {
    public static void main(String[] args) {
        EmailReader emailReader = new EmailReader();

        Dataset<Email> temp = emailReader.getEmailDataset("data/maildir/allen-p/straw");
        temp.printSchema();
        temp.show();



    }
}
