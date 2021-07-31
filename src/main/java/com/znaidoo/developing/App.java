package com.znaidoo.developing;

import java.sql.SQLException;
import java.util.List;

public class App 
{
    public static void main( String[] args ){
        MyUtils m = new MyUtils();
        try {
            m.createConnection();
            m.createStatement();
            m.createSchema("inMemory");
            m.useSchema();
            m.createTableRoles();
            m.createTableDirections();
            m.createTableProjects();
            m.createTableEmployee();

            m.insertTableRoles("Developer");
            m.insertTableRoles("DevOps");
            m.insertTableRoles("QC");
            m.insertTableDirections("Java");
            m.insertTableDirections("Python");
            m.insertTableDirections(".Net");

            m.insertTableProjects("MoonLight", "Java");
            m.insertTableProjects("Sun", "Java");
            m.insertTableProjects("Mars", "Python");

            m.insertTableEmployee("Ivan", "Developer", "MoonLight");
            m.insertTableEmployee("Petro", "Developer", "Sun");
            m.insertTableEmployee("Stepan", "Developer", "Mars");
            m.insertTableEmployee("Andriy", "DevOps", "MoonLight");
            m.insertTableEmployee("Vasyl", "Developer", "Mars");
            m.insertTableEmployee("Ira", "Developer", "MoonLight");
            m.insertTableEmployee("Anna", "QC", "MoonLight");
            m.insertTableEmployee("Olya", "QC", "Sun");
            m.insertTableEmployee("Maria", "QC", "Mars");

            List<String> dev = m.getAllDevelopers();
            for (String s : dev) {
                System.out.println(s);
            }

        }catch (SQLException e){
            e.getStackTrace();
        }
    }
}
