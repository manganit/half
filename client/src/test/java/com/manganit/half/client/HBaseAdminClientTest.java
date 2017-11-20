package com.manganit.half.client;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for HBaseAdminClient.
 */
public class HBaseAdminClientTest
        extends TestCase {

  /**
   * Create the test case
   *
   * @param testName name of the test case
   */
  public HBaseAdminClientTest(String testName) {
    super(testName);
  }

  /**
   * @return the suite of tests being tested
   */
  public static Test suite() {
    return new TestSuite(HBaseAdminClientTest.class);
  }

  /**
   * Rigourous Test :-)
   */
  public void testApp() {
    assertTrue(true);
  }

  public void testHBaseAdminClient() {
//    try {
//      String tablename = "scores";
//      String[] familys = {"grade", "course"};
//      HBaseAdminClient admin = new HBaseAdminClient();
//      
//      admin.creatTable(tablename, familys);
//
//      // add record zkb
//      admin.addRecord(tablename, "zkb", "grade", "", "5");
//      admin.addRecord(tablename, "zkb", "course", "", "90");
//      admin.addRecord(tablename, "zkb", "course", "math", "97");
//      admin.addRecord(tablename, "zkb", "course", "art", "87");
//      // add record baoniu
//      admin.addRecord(tablename, "baoniu", "grade", "", "4");
//      admin.addRecord(tablename, "baoniu", "course", "math", "89");
//
//      System.out.println("===========get one record========");
//      admin.printOneRecord(tablename, "zkb");
//
//      System.out.println("===========show all record========");
//      admin.printAllRecords(tablename);
//
//      System.out.println("===========del one record========");
//      admin.delRecord(tablename, "baoniu");
//      admin.printAllRecords(tablename);
//
//      System.out.println("===========show all record========");
//      admin.printAllRecords(tablename);
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
  }
}
