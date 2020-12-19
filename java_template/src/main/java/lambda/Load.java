package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.GetObjectRequest;
import saaf.Inspector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.util.Properties;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.CSVFormat;


public class Load implements RequestHandler<HashMap<String, Object>, HashMap<String, Object>> {

    public CSVParser readCSV(
        Inspector inspector, 
        AmazonS3 s3Client, 
        String bucketName, 
        String fileName
    ) {
        S3Object s3Object = s3Client.getObject(new GetObjectRequest(bucketName, fileName));
        InputStream csvInputStream = s3Object.getObjectContent();
        InputStreamReader csvInputStreamReader = new InputStreamReader(csvInputStream);
        CSVParser csvParser = null;
        try {
            csvParser = CSVFormat.EXCEL.withHeader().parse(csvInputStreamReader);
        } catch (IOException e) {
            inspector.addAttribute("error", "Error while reading CSV file from S3");
            inspector.addAttribute("stackTrace", e.getStackTrace());
        }

        return csvParser;
    }

    public void writeSQL(
        Inspector inspector, 
        CSVParser csvParser,
        String dbLink, 
        String dbName, 
        String username, 
        String password, 
        String tableName
    ) {
        Properties properties = new Properties();
        properties.setProperty("user", username);
        properties.setProperty("password", password);
        properties.setProperty("useSSL", "false");

        int batchSize = 1;

        try {
            Connection connection = DriverManager.getConnection("jdbc:mysql://" + dbLink + ":3306/" + dbName, properties);
            PreparedStatement preparedStatement = connection.prepareStatement("DROP TABLE IF EXISTS `" + tableName + "`;");
            preparedStatement.execute();
            preparedStatement = connection.prepareStatement(
                "CREATE TABLE " + tableName
                + " (Region VARCHAR(40), Country VARCHAR(40), `Item Type` VARCHAR(40), `Sales Channel` VARCHAR(40),`Order Priority` VARCHAR(40), `Order Date` VARCHAR(40),`Order ID` INT PRIMARY KEY, `Ship Date` VARCHAR(40), `Units Sold` INT,`Unit Price` DOUBLE, `Unit Cost` DOUBLE, `Total Revenue` DOUBLE, `Total Cost` DOUBLE, `Total Profit` DOUBLE, `Order Processing Time` INT, `Gross Margin` FLOAT) ENGINE = MyISAM;"
            );
            preparedStatement.execute();
            String insertSQL = "INSERT INTO " + tableName 
                + " (Region, Country, `Item Type`, `Sales Channel`, `Order Priority`, `Order Date`, `Order ID`, `Ship Date`, `Units Sold`, `Unit Price`, `Unit Cost`, `Total Revenue`, `Total Cost`, `Total Profit`, `Order Processing Time`, `Gross Margin`) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,? ,?)";
            PreparedStatement statement = connection.prepareStatement(insertSQL);

            // Create header
            ArrayList<String> header = new ArrayList<String>();
            header.add("Region");
            header.add("Country");
            header.add("Item Type");
            header.add("Sales Channel");
            header.add("Order Priority");
            header.add("Order Date");
            header.add("Order ID");
            header.add("Ship Date");
            header.add("Units Sold");
            header.add("Unit Price");
            header.add("Unit Cost");
            header.add("Total Revenue");
            header.add("Total Cost");
            header.add("Total Profit");
            header.add("Order Processing Time");
            header.add("Gross Margin");
            
            List<CSVRecord> csvList;
            csvList = csvParser.getRecords();
            for (int row_i=0; row_i<csvList.size(); row_i++) {
                CSVRecord csvRecord = csvList.get(row_i);
                for (int key_i=0; key_i<header.size(); key_i++) {
                    String key = header.get(key_i);
                    statement.setString(key_i + 1, csvRecord.get(key));
                }
                statement.addBatch();
                if ((row_i+1) % batchSize == 0) {
                    statement.executeBatch();
                }
            }
            statement.executeBatch();
            statement.close();
            connection.close();
        } catch (SQLException | IOException e) {
            inspector.addAttribute("error", "Error while writing SQL to database");
            inspector.addAttribute("stackTrace", e.getStackTrace());
        }
    }

    public HashMap<String, Object> handleRequest(HashMap<String, Object> request, Context context) {
        
        Inspector inspector = new Inspector();

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        String bucketName = (String) request.get("bucketName");
        String fileName = (String) request.get("fileName");
        String dbLink = (String) request.get("dbLink");
        String dbName = (String) request.get("dbName");
        String username = (String) request.get("username");
        String password = (String) request.get("password");
        String tableName = (String) request.get("tableName");

        CSVParser csvParser = readCSV(inspector, s3Client, bucketName, fileName);
        writeSQL(inspector, csvParser, dbLink, dbName, username, password, tableName);

        return inspector.finish();
    }
}
