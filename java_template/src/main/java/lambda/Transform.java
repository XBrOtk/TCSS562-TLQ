package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.GetObjectRequest;
import saaf.Inspector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.CSVFormat;


public class Transform implements RequestHandler<HashMap<String, Object>, HashMap<String, Object>> {

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

    public String processCSV(CSVParser csvParser) {
        Iterator<CSVRecord> csvIterator = csvParser.iterator();
        ArrayList<String> uniqueOrderIDList = new ArrayList<String>();
        StringBuilder stringBuilder = new StringBuilder();

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

        stringBuilder.append(String.join(",", header) + "\n");

        // Process fields
        while (csvIterator.hasNext()) {
            CSVRecord csvRecord = csvIterator.next();

            // Remove duplicated order IDs
            String orderID = csvRecord.get("Order ID");
            if (!uniqueOrderIDList.contains(orderID)) {
                uniqueOrderIDList.add(orderID);
                ArrayList<String> row = new ArrayList<String>();

                // Add fields
                for (String key : header) {
                    if (key.equals("Order Priority")) { // Correct order priority
                        String orderPriority = csvRecord.get("Order Priority"); // Add Order Priority
                        if (orderPriority.equals("L")) {
                            row.add("Low");
                        }
                        else if (orderPriority.equals("M")) {
                            row.add("Medium");
                        }
                        else if (orderPriority.equals("C")) {
                            row.add("Critical");
                        }
                        else if (orderPriority.equals("H")) {
                            row.add("High");
                        } else {
                            row.add("Unknown");
                        }
                    } else if (key.equals("Order Processing Time")) { // Add "Order Processing Time"
                        String[] orderDate = csvRecord.get("Order Date").split("/");
                        String[] shipDate = csvRecord.get("Ship Date").split("/");

                        Integer orderProcessingTime = (Integer.parseInt(shipDate[2]) - Integer.parseInt(orderDate[2]))*365 + 
                            (Integer.parseInt(shipDate[0]) - Integer.parseInt(orderDate[0]))*30 + 
                                (Integer.parseInt(shipDate[1]) - Integer.parseInt(orderDate[1]));
                        row.add(orderProcessingTime.toString());
                    } else if (key.equals("Gross Margin")) { // Add Gross Margin
                        Float grossMarginFloat = Float.parseFloat(csvRecord.get("Total Profit")) / Float.parseFloat(csvRecord.get("Total Revenue"));
                        DecimalFormat decimalFormat = new DecimalFormat("0.00");
                        String grossMargin = decimalFormat.format(grossMarginFloat);
                        row.add(grossMargin);
                    } else {
                        row.add(csvRecord.get(key));
                    }
                }

                stringBuilder.append(String.join(",", row) + "\n");
            }
        }

        return stringBuilder.toString();
    }

    public void writeCSV(
        AmazonS3 s3Client, 
        String bucketName, 
        String fileName, 
        String csvString
    ) {
        byte[] csvBytes = csvString.getBytes();
        InputStream csvInputStream = new ByteArrayInputStream(csvBytes);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(csvBytes.length);
        meta.setContentType("text/plain");

        s3Client.putObject(bucketName, fileName, csvInputStream, meta);
    }

    public HashMap<String, Object> handleRequest(HashMap<String, Object> request, Context context) {
        
        Inspector inspector = new Inspector();

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().build();
        String bucketName = (String) request.get("bucketName");
        String fileName = (String) request.get("fileName");

        CSVParser csvParser = readCSV(inspector, s3Client, bucketName, fileName);
        String csvString = processCSV(csvParser);
        writeCSV(s3Client, bucketName, "Processed_" + fileName, csvString);

        return inspector.finish();
    }
}
