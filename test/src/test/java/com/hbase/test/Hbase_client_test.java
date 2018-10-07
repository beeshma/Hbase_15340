package com.hbase.test;



import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.Assert;
import org.apache.hadoop.hbase.master.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
/*import org.apache.hadoop.hbase.regionserver.*;*/
/*import org.apache.hadoop.hbase.regionserver.HRegionServer;*/

/**
 * Test for Hbase_client  
 * 
 */
public class Hbase_client_test {
  
  final static Logger logger = Logger.getLogger(Hbase_client_test.class);
  private  static  Configuration cf;
  private static  Connection conc;
  
  
  
  public Hbase_client_test()  throws IOException
  {
    
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.setInt("WAIT_ON_REGIONSERVERS_MINTOSTART",3);
    conf.setInt("WAIT_ON_REGIONSERVERS_MAXTOSTART",3);
    
    Hbase_client_test.cf=conf;
    this.conc=ConnectionFactory.createConnection(cf);

  }
  


public static void deleteTableIfAnyandcreate(TableName tableName,byte[] familyname) throws IOException {
  Hbase_client HC= new Hbase_client();
 
 
  try {
    deleteTable(tableName);
  } catch (TableNotFoundException e) {
    
    System.out.println("Table not present so trying to create ");    
  }
  HC.createTable(tableName,familyname,cf);
  
}



public static void deleteTable(TableName tableName) throws IOException {
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
    boolean ret= false;
    Connection con = ConnectionFactory.createConnection(cf);
    Admin admin =con.getAdmin();
     List<TableDescriptor> ta=admin.listTableDescriptors();
 
    
    for (TableDescriptor i : ta)
    {
       System.out.println(i.getTableName().toString());
      if (t.getNameAsString().equals(i.getTableName().toString()))
      {
        
        System.out.println(i.getTableName().toString()+"==> This is the table what we looking for");
        ret= true;
      }
      
      
    }
    
    return ret;
        
}
  


  @Before
public void  Tablecreation() throws IOException
{
   
    TableName testname=TableName.valueOf("mytableaa1x123x1");
    byte[] familyname= String.valueOf("mycol").getBytes();
    
    if (! isTablepresent(testname))
    {
      
      System.out.println("Tb psent");
      
     deleteTableIfAnyandcreate(testname,familyname);
    } 
    
      
}
/*
  @Before
  public void MultiRegionTableCreate() throws IOException
  {
    Hbase_client HCM= new Hbase_client();
    TableName tablename=TableName.valueOf("MultiRegion");
    
    if (isTablepresent(tablename))
    {
      deleteTable(tablename);
    }
    
    byte[] startKey = Bytes.toBytes("aaaaa");
    byte[] endKey = Bytes.toBytes("zzzzz");
    byte[][] splitKeys = Bytes.split(startKey, endKey, 1);
    HCM.createMultiRegionTable(tablename, splitKeys, cf);
     
    
  }
  */
  
 /*@Test
 
 public void testNumberofRegoins() throws IOException
 {
   
   Hbase_client HCM= new Hbase_client();
   TableName tablename=TableName.valueOf("MultiRegion");
      
   
   
   
 }*/
  
 
  @Test
  public void testPrintHelloWorld() throws IOException {

   /* HBaseAdmin HAD=new HBaseAdmin(conc);*/
    
    Admin ad=conc.getAdmin();
     
    List<RegionInfo> Li= new ArrayList <RegionInfo>();
    
    Hbase_client HC= new Hbase_client();
    TableName tablename=TableName.valueOf("MultiRegion");
    if (isTablepresent(tablename))
    {
      deleteTable(tablename);
    }
    byte[] startKey = Bytes.toBytes("aaaaa");
    byte[] endKey = Bytes.toBytes("zzzzz");
    byte[][] splitKeys = Bytes.split(startKey, endKey, 1);
    HC.createMultiRegionTable(tablename, splitKeys, cf);
    
      Li=ad.getRegions(tablename);
      
      for(RegionInfo ri:Li)
      {
        System.out.println(ri.getStartKey());
        
        System.out.println( ri.getRegionName());
      }
     
    Assert.assertEquals("3", Li.size());

  }


}
