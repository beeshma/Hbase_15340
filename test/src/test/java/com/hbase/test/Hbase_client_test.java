package com.hbase.test;



import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.Assert;
import org.apache.hadoop.hbase.master.*;



public class Hbase_client_test {
  
  
  private  Configuration cf;
  private Connection conc;
  
  
  
  public Hbase_client_test()  throws IOException
  {
    
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.setInt("WAIT_ON_REGIONSERVERS_MINTOSTART",3);
    conf.setInt("WAIT_ON_REGIONSERVERS_MAXTOSTART",3);
    
    this.cf=conf;
    this.conc=ConnectionFactory.createConnection(cf);

  }
  


public void deleteTableIfAnyandcreate(TableName tableName,byte[] familyname) throws IOException {
  Hbase_client HC= new Hbase_client();
 
 
  try {
    deleteTable(tableName);
  } catch (TableNotFoundException e) {
    System.out.println(e.getMessage());
    System.out.println("Table not present");
    HC.createTable(tableName,familyname,cf);  
  }
}

public void deleteTable(TableName tableName) throws IOException {
  try {
    
          Admin admin =conc.getAdmin();
          admin.disableTable(tableName);
          admin.deleteTable(tableName);
         System.out.println("Table got deleted successfuly");
    
  } catch (TableNotEnabledException e) {

  }
}


public boolean  isTablepresent(TableName t) throws IOException
{
    
    Connection con = ConnectionFactory.createConnection(cf);
    
    
    String s=   con.getTable(t).getName().toString();
   // con.getConfiguration().
        
  // if (s.equals(t.getNameAsString()))
     if (s.equals("mytableaa1x123x1"))
   
   {
     System.out.println(s);
     System.out.println("Table is present");
       return true;
   }
   else
   {
     System.out.println("Table is not present");
     return false;
   }
}
  


  @Before
public void  Tablecreation() throws IOException
{

    TableName testname=TableName.valueOf("mytableaa1x123x1");
    byte[] familyname= String.valueOf("mycol").getBytes();
    
    if (isTablepresent(testname))
    {
      
      System.out.println("Tb psent");
      
     //deleteTableIfAnyandcreate(testname,familyname);
    } 
    
   else
    {
     System.out.println("Tb nt psent");
      //HC.createTable(testname,familyname,cf);
    }
        
}

   
  @Test
  public void testPrintHelloWorld() {

    Assert.assertEquals("Hello World", "Hello World");

  }
  

}