package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import saaf.Inspector;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.sql.Connection;
import java.util.Properties;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


public class Query implements RequestHandler<HashMap<String, Object>, HashMap<String, Object>> {

    public Connection connectDatabase(
        Inspector inspector,
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

        Connection connection = null;
        try {
            connection = DriverManager.getConnection("jdbc:mysql://" + dbLink + ":3306/" + dbName, properties);
        } catch (SQLException e) {
            inspector.addAttribute("error", "Error while connecting to database");
            inspector.addAttribute("stackTrace", e.getStackTrace());
        }

        return connection;
    }

    public void filterSQL(
        Inspector inspector, 
        Statement statement,
        HashMap<String, String> fields,
        HashMap<String, Object> result,
        String tableName
    ) {
        try {
            String filterQuery = "SELECT sr.* FROM " + tableName + " sr";

            // Get all elements from fields
            ArrayList<String> keyList = new ArrayList<String>();
            Iterator<String> keyIterator = fields.keySet().iterator();
            while (keyIterator.hasNext()){
                keyList.add(keyIterator.next());
            }

            // Construct the filter query
            if (keyList.size() > 0) {
                filterQuery = filterQuery + " WHERE sr.`" + keyList.get(0) + "`='" + fields.get(keyList.get(0)) + "'";
                for (int i=1; i<keyList.size(); i++) {
                    filterQuery = filterQuery + "AND sr.`" + keyList.get(0) + "`='" + fields.get(keyList.get(i)) + "' ";
                }
            }
            filterQuery = filterQuery + ";";

            // Execute and store result data
            JSONArray resultJsonArray = new JSONArray(); 
            ResultSet queryResult = statement.executeQuery(filterQuery);
            JSONObject row = new JSONObject();

			while (queryResult.next()) {
                row.put("Order Date", queryResult.getString("Order Date"));
                row.put("Order ID", queryResult.getInt("Order ID"));
				row.put("Ship Date", queryResult.getString("Ship Date"));
				row.put("Units Sold", queryResult.getDouble("Units Sold"));
				row.put("Unit Price", queryResult.getDouble("Unit Price"));
				row.put("Unit Cost", queryResult.getDouble("Unit Cost"));
				row.put("Total Revenue", queryResult.getDouble("Total Revenue"));
				row.put("Total Cost", queryResult.getDouble("Total Cost"));
				row.put("Total Profit", queryResult.getDouble("Total Profit"));
				row.put("Order Processing Time", queryResult.getInt("Order Processing Time"));
				row.put("Gross Margin", queryResult.getDouble("Gross Margin"));

				resultJsonArray.add(row);
			}
			result.put("filteredItems", resultJsonArray);

        } catch (SQLException e) {
            inspector.addAttribute("error", "Error while filtering SQL from database");
            inspector.addAttribute("stackTrace", e.getStackTrace());
        }
    }

    public void aggregateSQL(
        Inspector inspector,
        Statement statement,
        HashMap<String, String> fields,
        HashMap<String, Object> result,
        String tableName
    ) {
    	String aggregateQuery = "SELECT AVG(sr.`Order Processing Time`) as `Avg Order Processing Time`, AVG(sr.`Gross Margin`) as `Avg Gross Margin`, AVG(sr.`Units Sold`) as `Avg Units Sold`, MAX(sr.`Units Sold`) as `Max Units Sold`, MIN(sr.`Units Sold`) as `Min Units Sold`, SUM(sr.`Units Sold`) as `Sum Units Sold`, SUM(sr.`Total Revenue`) as `Sum Total Revenue`, SUM(sr.`Total Profit`) as `Sum Total Profit`, COUNT(*) as `Total Orders` FROM " +  tableName + " sr ";

    	// Get all elements from fields
    	ArrayList<String> keyList = new ArrayList<String>();
        Iterator<String> keyIterator = fields.keySet().iterator();
        while (keyIterator.hasNext()){
        	keyList.add(keyIterator.next());
        }

        // Construct the aggregate query
        if (keyList.size() > 0) {
        	aggregateQuery = aggregateQuery + "WHERE sr.`" + keyList.get(0) + "`='" + fields.get(keyList.get(0)) + "' ";
            for (int i=1; i<keyList.size(); i++) {
            	aggregateQuery = aggregateQuery + "AND sr.`" + keyList.get(i) + "`='" + fields.get(keyList.get(i)) + "' ";
            }
        }
        aggregateQuery = aggregateQuery + ";";

        // Execute and store result data
        try {
            ResultSet queryResult = statement.executeQuery(aggregateQuery);
			while(queryResult.next()) {
				result.put("Avg Order Processing Time", queryResult.getDouble("Avg Order Processing Time"));
				result.put("Avg Gross Margin", queryResult.getDouble("Avg Gross Margin"));
				result.put("Avg Units Sold", queryResult.getDouble("Avg Units Sold"));
				result.put("Max Units Sold", queryResult.getDouble("Max Units Sold"));
				result.put("Min Units Sold", queryResult.getDouble("Min Units Sold"));
				result.put("Sum Units Sold", queryResult.getDouble("Sum Units Sold"));
				result.put("Sum Total Revenue", queryResult.getDouble("Sum Total Revenue"));
				result.put("Sum Total Profit", queryResult.getDouble("Sum Total Profit"));
				result.put("Total Orders", queryResult.getDouble("Total Orders"));
			}
		} catch (SQLException e) {
            inspector.addAttribute("error", "Error while aggregating SQL from database");
            inspector.addAttribute("stackTrace", e.getStackTrace());
		}
    }

    public HashMap<String, Object> handleRequest(HashMap<String, Object> request, Context context) {
        
        Inspector inspector = new Inspector();

        String dbLink = (String) request.get("dbLink");
        String dbName = (String) request.get("dbName");
        String username = (String) request.get("username");
        String password = (String) request.get("password");
        String tableName = (String) request.get("tableName");

        // Get requested fields
    	HashMap<String, String> fields = new HashMap<String, String>();
        if (request.containsKey("Region")) {
        	fields.put("Region", (String) request.get("Region"));
        }
        if (request.containsKey("Country")) {
        	fields.put("Country", (String) request.get("Country"));
        }
        if (request.containsKey("Item Type")) {
        	fields.put("Item Type", (String) request.get("Item Type"));
        }
        if (request.containsKey("Sales Channel")) {
        	fields.put("Sales Channel", (String) request.get("Sales Channel"));
        }
        if (request.containsKey("Order Priority")) {
        	fields.put("Order Priority", (String) request.get("Order Priority"));
        }
        HashMap<String, Object> result = new HashMap<String, Object>();

        // Connect to database
        try {
            Connection connection = connectDatabase(inspector, dbLink, dbName, username, password, tableName);
            Statement statement = connection.createStatement();
            
            // Filter and aggregate results
            filterSQL(inspector, statement, fields, result, tableName);
            aggregateSQL(inspector, statement, fields, result, tableName);
            inspector.addAttribute("result", result);

            statement.close();
            connection.close();
        } catch (SQLException e) {
            inspector.addAttribute("error", "Error while aggregating SQL from database");
            inspector.addAttribute("stackTrace", e.getStackTrace());
        }
        
        return inspector.finish();
    }
}
