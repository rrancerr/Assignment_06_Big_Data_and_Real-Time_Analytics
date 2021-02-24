# Assignment 6: Big Data and Real-Time Analytics
_“Enron Corporation was an American energy, commodities, and services company based in Houston, Texas. (…) Before its bankruptcy on December 2, 2001, Enron employed approximately 20,000 staff and was one of the world's major electricity, natural gas, communications and pulp and paper compa-nies, with claimed revenues of nearly $101 billion during 2000. (…)
“At the end of 2001, it was revealed that its reported financial condition was sustained by institu-tionalized, systematic, and creatively planned accounting fraud, known since as the Enron scandal. Enron has since become a well-known example of willful corporate fraud and corruption.”1
During their investigation, authorities secured e-mails created by Enron employees – a large data-base of e-mails which has since become known as the Enron Corpus. This real-world dataset2 fre-quently serves as a case study for text mining and text data analysis.
In this assignment, you will write a program for extracting (subsets of) e-mail data, transforming e-mail data in a format suitable for data analysis, and store the data in Parquet format. You will then write an application to use Spark and Spark SQL for analyzing the data. In order to run your programs, please use your local machine.
Note: This is an individual assignment. In order for your submission to be graded, you must explain and demonstrate your solution in a separate session. Make an appointment for that session by e-mail by 28 February 2021; the appointment can take place after the 28 February and will be held via Zoom. However, you should submit your source code on Moodle by the deadline._
## 1. Data Preparation and Storage
### 1.1 Create a Maven project using the pom.xml from the sample projects and changing the arte-fact id.
See `pom.xml`
### 1.2 Create an Email class with the following fields: id, date, from, recipients, subject, body. The id, date, from and recipients fields are mandatory. The subject and body can be empty. The recipients field should be an array of strings.
See `src/main/java/at/jku/dke/dwh/enronassignment/objects/Email.java`
### 1.3 Download and unpack the Enron e-mail dataset3. The unpacked directory contains further directories, which contain the actual e-mails (or other subdirectories). You should create a function that takes a path (as string) as input, reads all e-mail files, and returns a dataset of Email objects. You can assume that the path contains the e-mail files, and that the e-mail files are not located in subdirectories of the specified path (if they are, these files are not loaded by the function).
####   Note: The recipients field subsumes an e-mail’s To, Cc, and Bcc as well X-To, X-cc, and X-bcc fields. The e-mail body may run over multiple lines; preserve the line breaks. Transform the e-mail’s date field into a suitable date/time type.
See `at.jku.dke.dwh.enronassignment.preparation.EmailReader.getEmailDataset` 
### 1.4 Provide methods that convert between Dataset<Email> and Dataset<Row> (Data-frame).
See `at.jku.dke.dwh.enronassignment.util.Utils.convertToRowDataset` and `at.jku.dke.dwh.enronassignment.util.Utils.convertToEmailDataset`
### 1.5 Create a method that appends one Dataset<Email> to another Dataset<Email>.
See `at.jku.dke.dwh.enronassignment.util.Utils.concatenateDatasets`
### 1.6 Create a method that takes as inputs a Dataset<Email> and a path (as string), and stores the input dataset in parquet format at the specified location.
See `at.jku.dke.dwh.enronassignment.util.Utils.storeAsParquet`
### 1.7 Create a function that takes as input an array of strings that specify locations of parquet files with e-mail data and returns a single, integrated Dataset<Email> as result.
See `at.jku.dke.dwh.enronassignment.preparation.EmailReader.readFromParquetFiles`

## 2. Data Analysis with Spark and Spark SQL
### 2.1 Create a function that returns the average length and the average number of recipients for all the e-mails in a set of parquet files, specified as an array of strings, with a date value that falls within a specified interval following these steps:
#### a. Load a Dataset<Email> from parquet files that contain the e-mail data using the functions from the previous task.
#### b. Filter the records by date so that only the e-mails in the given interval remain.
#### c. From the filtered Dataset<Email>, create a new Dataset<Row> that represents a table with columns for the e-mail id and the length (number of words) of the e-mail. Register the data frame as a temporary table.
#### d. Using Spark SQL, calculate the average number of recipients and the average mes-sage length of e-mails.
#### e. Return the result as a JSON file with two fields, avgLength and avgNoOfRecipi-ents. The JSON file should be written at a specified location.
### 2.2 Create a function that computes the average number of recipients of an e-mail for each sender as well as the average length of an e-mail for each sender from all the e-mails in a set of parquet files with a date value in a given interval following these steps:
#### a. Load a Dataset<Email> from parquet files that contain the e-mail data using the functions from the previous task.
#### b. Filter the records by date so that only the e-mails in the given interval remain.
#### c. From the filtered Dataset<Email>, create a new Dataset<Row> for a table with columns for the e-mail id, the sender (From), the length (number of words) of the e-mail, and the number of recipients. Register the data frame as a temporary table.
#### d. Using Spark SQL, calculate the average number of recipients of an e-mail for each sender as well as the average length of an e-mail for each sender.
#### e. Return the result as a JSON file with fields for the sender, avgNoOfRecpients, and avgLength. The JSON file should be written at a specified location.
### 2.3 Demonstrate your functions using example calls.
See `at.jku.dke.dwh.enronassignment.run.Analyzer.task2point1`
and
`at.jku.dke.dwh.enronassignment.run.Analyzer.task2point2` 

## 3. E-Mail Generator
### 3.1 Download the email-generator-1.1-bin.tar.gz package from Moodle and unpack it. The pro-gram periodically writes out a fixed number of e-mails into an output directory.
See `email-generator-1.1.0`
### 3.2 You may change the output directory, that is, the location where the e-mail generator stores the generated e-mails. The -o option of the program determines the output directory: Set it to wherever you would like your e-mails generated.
See Changed the option to the `generator_output` directory in root.
### 3.3 Run the start script to have e-mails copied into the output directory.
Running the program with console

## 4. Data Analysis with Spark Streaming
### 4.1 Read from a text stream which uses the e-mail generator’s output directory as input.
See `at.jku.dke.dwh.enronassignment.run.Analyzer.task4point1`
### 4.2 Use the Email class to represent the e-mails from the ingested input stream.
### 4.3 Find the top-five senders of e-mails as measured by the total numbers of e-mails sent. Fur-thermore, find the top-five senders of e-mails as measured by the total number of words in all sent e-mails.
### 4.4 Find the top-five most used words in the last minute using window operations.
### 4.5 Append the outputs of your calculations to separate parquet files – one parquet file for the top-five senders overall, one parquet file for the top-five most used words in the last minute – at a specified location. Each entry in the parquet file should contain the time instant and time window, respectively, along with the requested information.