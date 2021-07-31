package com.znaidoo.developing;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MyUtils {
    private Connection connection;
    private Statement statement;
    private String schemaName;

    public Connection createConnection() throws SQLException {
        DriverManager.registerDriver(new org.h2.Driver());
        return connection = DriverManager.getConnection("jdbc:h2:mem:test", "", "");
    }

    public void closeConnection() throws SQLException {
        connection.close();
    }

    public Statement createStatement() throws SQLException {
        return statement = connection.createStatement();
    }
    public void closeStatement() throws SQLException {
        statement.close();
    }

    public void createSchema(String schemaName) throws SQLException {
        this.schemaName = schemaName;
        statement.execute("CREATE SCHEMA IF NOT EXISTS " + schemaName + ";");
    }

    public void dropSchema() throws SQLException {
        statement.execute("DROP SCHEMA IF EXISTS " + schemaName + ";");
    }

    public void useSchema() throws SQLException {
        statement.execute("SET SCHEMA " + schemaName + ";");
    }

    public void createTableRoles() throws SQLException {
        statement.execute(
                "CREATE TABLE Roles (" +
                        "id INT NOT NULL AUTO_INCREMENT, " +
                        "roleName VARCHAR(20), " +
                        "PRIMARY KEY (id));"
        );
    }

    public void createTableDirections() throws SQLException {
        statement.execute(
                "CREATE TABLE Directions (" +
                        "id INT NOT NULL AUTO_INCREMENT, " +
                        "directionName VARCHAR(20) NOT NULL, " +
                        "PRIMARY KEY (id));"
        );
    }

    public void createTableProjects() throws SQLException {
        statement.execute(
                "CREATE TABLE Projects (" +
                        "id INT NOT NULL AUTO_INCREMENT, " +
                        "projectName VARCHAR(20) NOT NULL, " +
                        "PRIMARY KEY (id), " +
                        "directionId INT, " +
                        "FOREIGN KEY (directionId) REFERENCES Directions(id));"
        );
    }

    public void createTableEmployee() throws SQLException {
        statement.execute(
                "CREATE TABLE Employee (" +
                        "id INT NOT NULL AUTO_INCREMENT, " +
                        "firstName VARCHAR(20) NOT NULL, " +
                        "roleId INT NOT NULL, " +
                        "projectId INT NOT NULL, " +
                        "PRIMARY KEY (id), " +
                        "FOREIGN KEY (roleId) REFERENCES Roles(id), " +
                        "FOREIGN KEY (projectId) REFERENCES Projects(id));"
        );
    }

    public void dropTable(String tableName) throws SQLException {
        statement.execute("DROP TABLE " + tableName + ";");
    }

    public void insertTableRoles(String roleName) throws SQLException {
        statement.execute(
                "INSERT INTO Roles (roleName) " +
                        "VALUES ('" + roleName + "');"
        );
    }

    public void insertTableDirections(String directionName) throws SQLException {
        statement.executeUpdate(
                "INSERT INTO Directions (directionName)" +
                        " VALUES ('" + directionName + "');"
        );
    }

    public void insertTableProjects(String projectName, String directionName) throws SQLException {
        int directionId = getDirectionId(directionName);
        statement.execute(
                "INSERT INTO Projects (projectName, directionId)" +
                        " VALUES ('" + projectName + "', '" + directionId + "');"
        );
    }

    public void insertTableEmployee(String firstName, String roleName, String projectName) throws SQLException {
        int roleId = getRoleId(roleName);
        int projectId = getProjectId(projectName);
        statement.execute(
                "INSERT INTO Employee (firstName, roleId, projectId)" +
                        " VALUES ('"+ firstName + "', '" + roleId + "', '" + projectId + "');"
        );
    }

    public int getRoleId(String roleName) throws SQLException {
        try(ResultSet resultSet = statement.executeQuery(
                "SELECT id FROM Roles WHERE roleName='" + roleName + "';")){
            return resultSet.next()
                    ? resultSet.getInt(1)
                    : -1;
        }
    }

    public int getDirectionId(String directionName) throws SQLException {
        try(ResultSet resultSet = statement.executeQuery(
                "SELECT id FROM Directions WHERE directionName='" + directionName + "';")){
            return resultSet.next()
                    ? resultSet.getInt(1)
                    : -1;
        }
    }

    public int getProjectId(String projectName) throws SQLException {
        try(ResultSet resultSet = statement.executeQuery(
                "SELECT id FROM Projects WHERE projectName='" + projectName + "';")){
            return resultSet.next()
                    ? resultSet.getInt(1)
                    : -1;
        }
    }

    public int getEmployeeId(String firstName) throws SQLException {
        try(ResultSet resultSet = statement.executeQuery(
                "SELECT id FROM Employee WHERE firstName='" + firstName + "';")){
            return resultSet.next()
                    ? resultSet.getInt(1)
                    : -1;
        }
    }

    public List<String> getAllRoles() throws SQLException {
        try(ResultSet resultSet = statement.executeQuery("SELECT * FROM roles;")){
            return new ArrayList<String>(){{
                while (resultSet.next()) add(resultSet.getString("roleName"));
            }};
        }
    }

    public List<String> getAllDirestion() throws SQLException {
        try(ResultSet resultSet = statement.executeQuery("SELECT * FROM directions;")){
            return new ArrayList<String>(){{
                while (resultSet.next()) add(resultSet.getString("directionName"));
            }};
        }
    }

    public List<String> getAllProjects() throws SQLException {
        try(ResultSet resultSet = statement.executeQuery("SELECT * FROM projects;")){
            return new ArrayList<String>(){{
                while (resultSet.next()) add(resultSet.getString("projectName"));
            }};
        }
    }

    public List<String> getAllEmployee() throws SQLException {
        try(ResultSet resultSet = statement.executeQuery("SELECT * FROM employee;")){
            return new ArrayList<String>(){{
                while (resultSet.next()) add(resultSet.getString("firstName"));
            }};
        }
    }

    public List<String> getAllDevelopers() throws SQLException {
        try(ResultSet resultSet = statement.executeQuery(
                    "SELECT firstName "
                            + "FROM Employee INNER JOIN Roles ON Employee.roleId = Roles.id "
                            + "WHERE Roles.roleName='Developer';")){
            return new ArrayList<String>(){{
                while (resultSet.next()) add(resultSet.getString("firstName"));
            }};
        }
    }

    public List<String> getAllJavaProjects() throws SQLException {
        try(ResultSet resultSet = statement.executeQuery(
                "SELECT projectName "
                        + "FROM Projects "
                        + "INNER JOIN "
                        + "Directions ON Projects.directionId = Directions.id "
                        + "WHERE Directions.directionName='Java';")){
            return new ArrayList<String>(){{
                while (resultSet.next()) add(resultSet.getString("projectName"));
            }};
        }
    }

    public List<String> getAllJavaDevelopers() throws SQLException {
        try(ResultSet resultSet = statement.executeQuery(
                "SELECT Employee.firstName FROM (((Employee INNER JOIN Roles ON Employee.roleId = Roles.id)"
                        +" INNER JOIN Projects ON Employee.projectId = Projects.id)"
                        +" INNER JOIN Directions ON Projects.directionId = Directions.id)"
                        +" WHERE Roles.roleName='Developer'and Directions.directionName='Java';")){
            return new ArrayList<String>(){{
                while (resultSet.next()) add(resultSet.getString(1));
            }};
        }
    }

}