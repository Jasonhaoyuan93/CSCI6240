package edu.neu.cs6620.hw1;

public class Main {
    public static void main(String[] args) {
        try{
            String fileLocation = args[0];
            int clusterCount = Integer.parseInt(args[1]);
            System.out.println("Application started with parameters: ");
            System.out.println("File location: "+fileLocation);
            System.out.println("Cluster count: "+clusterCount);
        }catch (Exception e){
            System.out.println("Application failed with cause: " + e.getMessage());
            System.exit(1);
        }

    }
}