/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main;

import hadoop.inputformat.WholeInputFormat;
import hadoop.inputformat.WholeSplitInputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import mapred.map.Map1;
import mapred.map.Map2;
import mapred.reduce.Reduce1;
import mapred.reduce.Reduce2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.server.balancer.Balancer;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.BalancerBandwidthCommand;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import utils.MrUtils;

/**
 *
 * @author eduardo
 */
public class Main {

    private Log log = LogFactory.getLog(Main.class);
    public static int countDir;
    private int timeTotal;
    private String support = "0.5";
    private int k = 1;
    public static int totalBlockCount;
    public static String user = "/user/eduardo/";
    public static String inputEntry = "input/T2.5I2D10N1500K.dobro";
    public static String clusterUrl = "hdfs://master/";
    public static long totalTransactionCount;
    public ArrayList<String> blocksIds;
    private String outputPartialName = user+"output-fase-1/partition";
    /*
    Valor do suporte para 1.000.000
    7500
    10.000
    12.500
    15.000
    17.500
    20.000
    */
    
    public Main() {
        countDir = 0;
        timeTotal = 0;
    }
 
    /**
     * 
     */
    public void job1(){
        
        Configuration c = new Configuration();
        Job job = null;
        try {
            job = Job.getInstance(c);
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        job.getConfiguration().set("fs.defaultFS", "hdfs://master/");
        job.setJobName("Fase 1");
        
        job.setJarByClass(Main.class);
        
        job.setMapperClass(Map1.class);
//        job.setCombinerClass(Combiner1.class);
        job.setReducerClass(Reduce1.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(WholeInputFormat.class);
        
        job.getConfiguration().set("count", String.valueOf(Main.countDir));
        job.getConfiguration().set("support", support);
        job.getConfiguration().set("outputPartialName", outputPartialName);
        job.getConfiguration().set("totalMaps", String.valueOf(this.totalBlockCount));
        job.getConfiguration().set("totalTransactions", String.valueOf(this.totalTransactionCount));
        for(int i = 1; i <= this.blocksIds.size(); i++){
        	job.getConfiguration().set("blockId"+i, this.blocksIds.get(i-1));
        }
        
        try {
        	FileInputFormat.setInputPaths(job, new Path(user+"input"));
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        System.out.println("Job 1 - CountDir: "+Main.countDir);
        
        FileOutputFormat.setOutputPath(job, new Path(user+"output"+Main.countDir));
        
        try {
            long ini = System.currentTimeMillis();
            int st = (job.waitForCompletion(true) ? 0 : 1);
            long fim = System.currentTimeMillis();
            
            long t = fim - ini;
            System.out.println("Tempo da fase 1: "+((double)t/1000));
            
            timeTotal += t;
            if(st == 1){
                System.exit(st);
            }
            
        } catch (InterruptedException | ClassNotFoundException | IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * 
     */
    public void job2(){
        
        Configuration c = new Configuration();
        
        Job job = null;
        try {
            job = Job.getInstance(c);
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        job.getConfiguration().set("fs.defaultFS", "hdfs://master/");
        job.setJobName("Fase 2");
        
        job.setJarByClass(Main.class);
        
        job.setMapperClass(Map2.class);
//        job.setCombinerClass(Reduce2.class);
        job.setReducerClass(Reduce2.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        k++;
        String fileCachedRead = user+"outputCached/outputMR"+(Main.countDir-1);
        String fileCachedWrited = user+"outputCached/outputMR"+Main.countDir;
        job.getConfiguration().set("count", String.valueOf(Main.countDir));
        job.getConfiguration().set("support", support);
        job.getConfiguration().set("k", String.valueOf(k));
        job.getConfiguration().set("fileCachedRead", fileCachedRead);
        job.getConfiguration().set("fileCachedWrited", fileCachedWrited);
          
        System.out.println("Job 2 - CountDir: "+Main.countDir);
        
        try {
           job.addCacheFile(new URI(fileCachedRead));
        } catch (URISyntaxException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        try {
            FileInputFormat.setInputPaths(job, new Path(user+"input"));
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        FileOutputFormat.setOutputPath(job, new Path(user+"output"+Main.countDir));
        try {
            long ini = System.currentTimeMillis();
            int st = (job.waitForCompletion(true) ? 0 : 1);
            long fim = System.currentTimeMillis();
            
            long t = fim - ini;
            System.out.println("Tempo da fase 2: "+((double)t/1000));
            
            timeTotal += t;
            if(st == 1){
                System.exit(st);
            }
            
        } catch (InterruptedException | ClassNotFoundException | IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
   


    public static void main(String[] args) throws IOException {
        Main m = new Main();
//        System.out.println(m.checkOutput(user+"output1"));
      
        MrUtils.initialConfig();
        m.blocksIds = MrUtils.extractBlocksIds();
        MrUtils.createIfNotExistOrClean(m.outputPartialName);
        
//        MrUtils.delOutDirs(m.user);
//        Main.countDir++;
//        m.job1();
        
/*        int l = 0;
        while(m.checkOutput(m.user+"output"+Main.countDir)){
            System.out.println("LOOP "+l++);
        	Main.countDir++;
        	m.job2();
        }
*/

        /*Remover os arquivos invertidos anteriores*/
//        m.delOutDirs(user+"");
//        m.delContentFiles("invert");
        
        double seg = ((double)m.timeTotal/1000);
        
        System.out.println("Tempo total: "+m.timeTotal+" mile ou "+seg+" segundos! ou "+seg/60+" minutos");
    }
   
}
