/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main;

import hadoop.inputformat.WholeSplitInputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author eduardo
 */
public class Main {

    private Log log = LogFactory.getLog(Main.class);
    public static int countDir;
    private int timeTotal;
    int support;
    int k = 1;
    String user = "/user/eduardo/";
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
        job.setOutputValueClass(IntWritable.class);
        
        String fileCached = user+"outputCached/outputMR"+(Main.countDir);
        job.getConfiguration().set("count", String.valueOf(Main.countDir));
        job.getConfiguration().set("support", String.valueOf(support));
        job.getConfiguration().set("fileCached", fileCached);
        
        try {
            WholeSplitInputFormat.setInputPaths(job, new Path(user+"input"));
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
        job.getConfiguration().set("support", String.valueOf(support));
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
    
   
    public boolean increseMapTask(Path file, Configuration c){
        try {
            FileSystem fs = FileSystem.get(c);
            
            FileStatus f = fs.getFileStatus(file);
            
            long blockSize = f.getBlockSize();
            long fileSize = f.getLen();
            
            if((4*blockSize) >= fileSize){
                return false;
            }
            
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return true;
        
    }
    
    /**
     * Deleta o diretório output
     */
    private void delDirs(String d) {
        
        log.info("Excluindo diretórios anteriores...");
        Configuration c = new Configuration();
        c.set("fs.defaultFS", "hdfs://master/");
        Path p = new Path(d);
        try {
            FileSystem fs = FileSystem.get(c);
            
            if(fs.isDirectory(p)){
                
                if(fs.delete(p, true)){
                    log.info("Excluido diretório -> "+p.getName());
                }else{
                    log.info("Nao foi possivel excluir "+p.getName());
                }
            }else{
                log.info(p.getParent()+""+p.getName()+" Nao eh um diretorio.");
                
                if(fs.isFile(p)){
                    log.info("Eh um arquivo, excluindo...");
                    
                    if(fs.delete(p, true)){
                        log.info("Excluindo arquivo -> "+p.getName());
                    }else{
                        log.info("Nao foi possivel excluir o arquivo -> "+p.getName());
                    }
                }
            }
            
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    private void delOutDirs(String d) {
        
        log.info("Excluindo diretórios anteriores...");
        Configuration c = new Configuration();
        c.set("fs.defaultFS", "hdfs://master/");
        Path p = new Path(d);
        Path aux;
        
        try {
            FileSystem fs = FileSystem.get(c);
            
            if(fs.isDirectory(p)){
                
                FileStatus[] ff = fs.listStatus(p);
                
                for(FileStatus f: ff){
                    aux = f.getPath();
                    
                    if(aux.getName().contains("output")){
                        
                        if(fs.delete(aux, true)){
                            log.info("Excluido diretório -> "+aux.getName());
                            
                        }else{
                            log.info("Nao foi possivel excluir "+aux.getName());
                        }
                    }
                }
                
            }
            
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    public void createTempDir(String d){
    	 Configuration c = new Configuration();
         c.set("fs.defaultFS", "hdfs://master/");
        try {
            FileSystem fs = FileSystem.get(c);
            
            if(fs.mkdirs(new Path(d))){
                log.info("Diretorio "+d+" criado com sucesso.");
            }else{
                log.info("Nao foi possivel criar o diretorio: "+d);
            }
            
        } catch (IOException ex) {
            Logger.getLogger(Reduce1.class.getName()).log(Level.SEVERE, null, ex);
        }
    
    }
    
    public void delContentFiles(String dir){
        Path p = new Path(dir);
        Configuration c = new Configuration();
        c.set("fs.defaultFS", "hdfs://master/");
        try {
            FileSystem fs = FileSystem.get(c);
            
            if(fs.isDirectory(p)){
                
                log.info(p.getName()+" eh um diretorio!");
                
                FileStatus[] f = fs.listStatus(p);
                log.info("Conteudo do diretorio: ");
                
                for(FileStatus ff: f){
                                        
                    log.info(ff.getPath().getName());
                    
                    if(ff.toString().contains("confIn")) continue;
                    
                    p = ff.getPath();
                    
                    if(fs.isFile(p)){
                        log.info("Deletando: "+p.getName());
                        if(fs.delete(p, true)){
                            
                            log.info("Deletado!");
                        }else{
                            log.info("Falha ao deletar.");
                        }
                    }
                }
            }
            
            
        }catch(IOException ex) {
            Logger.getLogger(Reduce1.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    public boolean checkOutput(String dir){
        
        Path p = new Path(dir);
        Path aux;
        Configuration c = new Configuration();
        c.set("fs.defaultFS", "hdfs://master/");
        System.out.println("Verificando diretório: "+dir);
        
        try{
            FileSystem fs = FileSystem.get(c);

                if(fs.isDirectory(p)){

                     FileStatus[] ff = fs.listStatus(p);

                     for(FileStatus f: ff){
                         
                         aux = f.getPath();
                         if(aux.getName().startsWith("part")){
                             System.out.println("Arquivos dentro do dir: "+aux.getName()+" "+f.getLen()/1024+"Kb ou "+f.getLen()+" bytes");
                             
                             if(f.getLen() > 0){
                                return true;
                                 
                             }else{
                                 return false;
                             }
                         }
                     }

                }else{
                    System.out.println("Não é um diretório: "+dir);
                    return false;
                }
        }catch(IOException e){
            System.out.println("ERROR: "+e);
        }
        System.out.println("Não contém part: "+dir);
        return false;
    }
    
    public boolean checkOutputMR(){
        String dir = user+"outputCached/outputMR"+Main.countDir;
        Path p = new Path(dir);
        
        Configuration c = new Configuration();
        c.set("fs.defaultFS", "hdfs://master/");
        System.out.println("Verificando diretório: "+dir);
        
        try {
            FileSystem fs = FileSystem.get(c);
            
            if(fs.getLength(p) > 0){
                System.out.println("O arquivo "+dir+" não é vazio! "+fs.getLength(p));
                return true;
            }
            System.out.println("O arquivo "+dir+" é vazio! "+fs.getLength(p));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        return false;
    }

    public static void main(String[] args) throws IOException {
        Main m = new Main();
//        System.out.println(m.checkOutput(user+"output1"));
        m.delOutDirs(m.user);
               
        Main.countDir++;
        m.job1();
        
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
