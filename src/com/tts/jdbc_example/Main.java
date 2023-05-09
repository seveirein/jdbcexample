package com.tts.jdbc_example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.sql.DataSource;

import org.postgresql.ds.PGSimpleDataSource;

//We need to add support for the postgres database.
//We do not have support built into our Java SE for 
//PostgresQL. So in the parlance of JDBC, we need to download
//a "DRIVER" for PostgresQL.

public class Main {
	private static final String username="seveirein";
	private static final String password="GTUW41CzxhFv";
	private static final String host="ep-misty-queen-448688.us-east-2.aws.neon.tech";
	private static final int port=5432; // default port for PostgreSQL
	private static final String database="neondb";
	
	public static DataSource getDataSource() {
		//We are going to setup a simple Postgres data source.
		PGSimpleDataSource source = new PGSimpleDataSource(); 
		source.setServerNames(new String[] {host});
		source.setDatabaseName(database);
		source.setPortNumbers(new int[] {port});
		source.setUser(username);
		source.setPassword(password);
		return source;				
	}
	
	public static Connection getConnectionFromDriverManager() throws SQLException {
		Connection connection = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", username);
		connectionProps.put("password", password);
		String jdbcURL = "jdbc:postgresql://"+host+":" + port + "/" + database;
		connection = DriverManager.getConnection(jdbcURL, connectionProps);
		return connection;
	}
	
	
	public static void buildDb(Connection connection) throws SQLException {
		//Statement can hold a SQL statement.
		Statement statement = connection.createStatement();
		statement.execute("CREATE TABLE Employees(" 
			+ "id INT NOT NULL,"
			+ "age INT NOT NULL,"
			+ "first VARCHAR (255),"
			+ "last VARCHAR (255)"
			+ ");");
		statement.execute("INSERT INTO Employees VALUES (100, 18, 'Zara', 'Ali');");
		statement.execute("INSERT INTO Employees VALUES (101, 25, 'Mahnaz', 'Fatma');");
		statement.execute("INSERT INTO Employees VALUES (102, 30, 'Zaid', 'Khan');");
		statement.execute("INSERT INTO Employees VALUES (103, 28, 'Sumit', 'Mittal');");
	}
	
	
	public static void main(String[] args) {
		Connection connection = null;
		try {
			Class.forName("org.postgresql.Driver");			
			
			//Example of getting connection from a Data Source
			//------------------------------------------------
			//connection = getDataSource().getConnection();		
			
			//Example of getting connection from DriverManager
			//------------------------------------------------
			connection = getConnectionFromDriverManager();
			
			//buildDb(connection);
			connection.setAutoCommit(false); //Turn off automatic commiting of every operation.
			

			Statement statement = connection.createStatement();
			
			//statement.execute is used for SQL commands that you
			//don't want to query results from.
			//If we look up Statement in the javadoc.
			ResultSet rs = statement.executeQuery("SELECT * from employees");
			
			//Now how do we read the results?
			while(rs.next()) {
				System.out.print("ID: " + rs.getInt("id"));
				System.out.print(", Age: " + rs.getInt("age"));
				System.out.print(", First: " + rs.getString("first"));
				System.out.println(", Last: " + rs.getString("last"));
			}			
			
			String SQL = "Update Employees SET age = ? WHERE id = ?";
			PreparedStatement myUpdate = null;
			try {
				myUpdate = connection.prepareStatement(SQL);
				myUpdate.setInt(1, 27);
				myUpdate.setInt(2, 110);				
				int updates = myUpdate.executeUpdate();
				if(updates != 1) {
					//Probably a mistake, let's throw an exception which also cause a rollback.
					throw new SQLException();
				}
			} finally {
				if(myUpdate != null) {
					myUpdate.close();
				}
			}
			
			connection.commit(); //There were no errors getting to this point.
			
		} catch(ClassNotFoundException e) {
			System.out.println("Postgres driver not available");
		} catch(SQLException e) {
			try {
				connection.rollback();
			} catch (SQLException e1) {
				//IF we have a problem rolling back, nothing more we can do.
			}
			System.out.println("SQL Exception thrown");
			System.out.println(e.getMessage());
			
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch(SQLException e) {
					System.out.println("Error closing database connection");
				}
			}
		}
	}
}
