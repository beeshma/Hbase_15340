package com.hbase.test;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.hbase.hbase


public class Hbase_client 
{
  
      public   void createTable (TableName tableName, byte[] family,Configuration conf) throws IOException
       {
         
       try (Connection con = ConnectionFactory.createConnection(conf);
           Admin admin = con.getAdmin())
       {
         System.out.println("table creation got reached");
         
         TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
             
             
             .addColumnFamily(ColumnFamilyDescriptorBuilder.of(family))
             .build();
         
        
         admin.createTable(desc);
       
         
         System.out.println("table got created");   
         
         }       
      }
      
      /* Create MultiRegion Table
       * @param1  tableName
       * @param2  families
       * @param3  conf
       */
      
      
      
      public  void createMultiRegionTable(TableName tableName,byte[][] families,byte[][] splitKeys,Configuration conf) throws IOException
      {
        
       
       try (Connection con = ConnectionFactory.createConnection(conf);
           Admin admin = con.getAdmin())
       {
         TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
         
         for(byte[] family : families )
         {
           builder.setColumnFamily(
             ColumnFamilyDescriptorBuilder
                 .newBuilder(family)
                 .build());
         }                       
         TableDescriptor desc=  builder.build();
         admin.createTable(desc,splitKeys);           
             
         }  
        
        }
     
      }
