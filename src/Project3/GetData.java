package Project3;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GetData {
	public List<Comparable[]> getTuples(String tableName,int limit) {
		List<Comparable[]> resultRows = new ArrayList<>();
		String jdbcUrl = "jdbc:postgresql://localhost:5432/pagila_db";
	    String username = "postgres";
	    String password = "root";
	    try {
	    	// Register the PostgreSQL JDBC driver
	    	Class.forName("org.postgresql.Driver");
	    	// Establish the database connection
	    	Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
	    	if (connection != null) {
	    		System.out.println("Connected to the PostgreSQL database!");
	    		// SQL query to select data from a table
	    		
	    		String sqlQuery = "SELECT * FROM "+tableName+" limit "+limit;
	    		PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
	    		// Execute the query
	    		ResultSet resultSet = preparedStatement.executeQuery();
	    		// Process the results
	    		ResultSetMetaData metaData = resultSet.getMetaData();
	    		int columnCount = metaData.getColumnCount();
	    		
	    		while (resultSet.next()) {
	    		    // Access table columns by index
	    		    Comparable[] rowArray = new Comparable[columnCount];
	    		    for (int i = 1; i <= columnCount; i++) {
	    		        String columnName = metaData.getColumnName(i);
	    		        Object value = resultSet.getObject(i);
	    		        // Convert the value to the appropriate type
	    		        if (value instanceof String) {
	    		            rowArray[i - 1] = (String) value;
	    		        } else if (value instanceof Integer) {
	    		            rowArray[i - 1] = (Integer) value;
	    		        } else if (value instanceof Double) {
	    		            rowArray[i - 1] = (Double) value;
	    		        } else if (value instanceof Boolean) {
	    		            rowArray[i - 1] = (Boolean) value;
	    		        } else if (value instanceof Date) {
	    		            // Convert Date to the desired string format
	    		            SimpleDateFormat dateFormat = new SimpleDateFormat("M/d/yy");
	    		            rowArray[i - 1] = dateFormat.format((Date) value);
	    		        } else {
	    		            // Handle other data types as needed
	    		        	if(value == null) {
	    		        		value = "null";
	    		        	}
	    		            rowArray[i - 1] = value.toString();
	    		        }
	    		    }
	    		    resultRows.add(rowArray);
	    		}

	    		// Close resources
	    		resultSet.close();
	    		preparedStatement.close();
	    		connection.close();
	    		
	        }
	    	else {
	    		System.out.println("Failed to connect to the database.");
	        }
	    	return resultRows;
	    } 
	    catch (ClassNotFoundException | SQLException e) {
	    	e.printStackTrace();
	    	return null;
	    }
    }
}

 

 

 

 