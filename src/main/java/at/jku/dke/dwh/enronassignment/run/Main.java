package at.jku.dke.dwh.enronassignment.run;

import org.apache.log4j.Logger;

/***
 * Main class - to run the program.
 *
 * @author Marco Stadler
 */


public class Main {
    private static final Logger LOGGER = Logger.getLogger(Main.class);

    public static void main(String[] args) {
        LOGGER.info("Starting...");

//        Analyzer.readEnronDemo();
//        Analyzer.readParquetDemo();
//        Analyzer.task2point1();
        Analyzer.task2point2();
//        Analyzer.task4point1();
    }
}
