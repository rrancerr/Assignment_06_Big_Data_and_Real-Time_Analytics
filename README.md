# Assignment 6: Big Data and Real-Time Analytics
_“Enron Corporation was an American energy, commodities, and services company based in Houston, Texas. (…) Before its bankruptcy on December 2, 2001, Enron employed approximately 20,000 staff and was one of the world's major electricity, natural gas, communications and pulp and paper compa-nies, with claimed revenues of nearly $101 billion during 2000. (…)
“At the end of 2001, it was revealed that its reported financial condition was sustained by institu-tionalized, systematic, and creatively planned accounting fraud, known since as the Enron scandal. Enron has since become a well-known example of willful corporate fraud and corruption.”1
During their investigation, authorities secured e-mails created by Enron employees – a large data-base of e-mails which has since become known as the Enron Corpus. This real-world dataset2 fre-quently serves as a case study for text mining and text data analysis.
In this assignment, you will write a program for extracting (subsets of) e-mail data, transforming e-mail data in a format suitable for data analysis, and store the data in Parquet format. You will then write an application to use Spark and Spark SQL for analyzing the data. In order to run your programs, please use your local machine.
Note: This is an individual assignment. In order for your submission to be graded, you must explain and demonstrate your solution in a separate session. Make an appointment for that session by e-mail by 28 February 2021; the appointment can take place after the 28 February and will be held via Zoom. However, you should submit your source code on Moodle by the deadline._
## Data Preparation and Storage
### 1. Create a Maven project using the pom.xml from the sample projects and changing the arte-fact id.
See `pom.xml`
### 2. Create an Email class with the following fields: id, date, from, recipients, subject, body. The id, date, from and recipients fields are mandatory. The subject and body can be empty. The recipients field should be an array of strings.
See `src/main/java/at/jku/dke/dwh/enronassignment/objects/Email.java`
### 3. Download and unpack the Enron e-mail dataset3. The unpacked directory contains further directories, which contain the actual e-mails (or other subdirectories). You should create a function that takes a path (as string) as input, reads all e-mail files, and returns a dataset of Email objects. You can assume that the path contains the e-mail files, and that the e-mail files are not located in subdirectories of the specified path (if they are, these files are not loaded by the function).
####   Note: The recipients field subsumes an e-mail’s To, Cc, and Bcc as well X-To, X-cc, and X-bcc fields. The e-mail body may run over multiple lines; preserve the line breaks. Transform the e-mail’s date field into a suitable date/time type.
See `at.jku.dke.dwh.enronassignment.preparation.EmailReader.getEmailDataset` 
### 4. Provide methods that convert between Dataset<Email> and Dataset<Row> (Data-frame).
See `at.jku.dke.dwh.enronassignment.util.Utils.convertToRowDataset` and `at.jku.dke.dwh.enronassignment.util.Utils.convertToEmailDataset`
### 5. Create a method that appends one Dataset<Email> to another Dataset<Email>.
See `at.jku.dke.dwh.enronassignment.util.Utils.concatenateDatasets`
### 6. Create a method that takes as inputs a Dataset<Email> and a path (as string), and stores the input dataset in parquet format at the specified location.
See `at.jku.dke.dwh.enronassignment.util.Utils.storeAsParquet`
### 7. Create a function that takes as input an array of strings that specify locations of parquet files with e-mail data and returns a single, integrated Dataset<Email> as result.
See `at.jku.dke.dwh.enronassignment.preparation.EmailReader.readFromParquetFiles`
