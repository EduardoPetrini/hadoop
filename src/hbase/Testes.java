/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package hbase;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;


/**
 *
 * @author eduardo
 */
public class Testes {
    
    public void createTable(String tableName){
        Configuration c = new Configuration();
        Configuration hc = HBaseConfiguration.create(c);
        
        try{
            HConnection con = HConnectionManager.createConnection(hc);                
            HBaseAdmin admin = new HBaseAdmin(hc);

            /*Criando tabela*/
            TableName tName = TableName.valueOf(tableName);
            admin.createTable(new HTableDescriptor(tName));
            
            /*Adicionano familia de colunas*/
            HColumnDescriptor[] fcol = new HColumnDescriptor[100];

            for (int i = 0; i < 100; i++) {
                fcol[i] = new HColumnDescriptor("fcol"+i);
                admin.addColumn(tName, fcol[i]);
            }
            try {
        
                admin.flush(Bytes.toBytes(tName.toString()));
            } catch (InterruptedException ex) {
                Logger.getLogger(Testes.class.getName()).log(Level.SEVERE, null, ex);
            }
        }catch(IOException e){
            
        }
    }
    
    public void addData(String tableName){
        
        Configuration c = new Configuration();
        Configuration hc = HBaseConfiguration.create(c);
        
        TableName tName = TableName.valueOf(tableName);
        try {
            /*Adicionano dados*/
            HTable table = new HTable(hc, tableName);
            
            Put put = new Put(Bytes.toBytes("line1"));
            
            for (int i = 0; i < 100; i++) {
                put.add(Bytes.toBytes("fcol"+i), Bytes.toBytes("q"+i), Bytes.toBytes("value"+i));
                table.put(put);
            }
            
            table.flushCommits();
            
        } catch (IOException ex) {
            Logger.getLogger(Testes.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    public void readData(String tableName){
        Configuration c = new Configuration();
        Configuration hc = HBaseConfiguration.create(c);
        
        TableName tName = TableName.valueOf(tableName);
        try {
            /*Adicionano dados*/
            HTable table = new HTable(hc, tableName);
            
            ResultScanner scan;
            Get get = new Get(Bytes.toBytes("line1"));
            Scan s = new Scan(get);
            
            scan = table.getScanner(s);
            
            Result r;
            while((r = scan.next()) != null){
                for (int i = 0; i < 100; i++) {
                    System.out.println(new String(r.getValue(Bytes.toBytes("fcol"+i), Bytes.toBytes("q"+i))));
                }
                
            }
            
        } catch (IOException ex) {
            Logger.getLogger(Testes.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void main(String[] args) {
        Testes t = new Testes();
        
//        t.createTable("TableInCode");
//        t.addData("TableInCode");
        t.readData("TableInCode");
        
    }
    
}
