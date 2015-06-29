/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.app;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;

/**
 *
 * @author hadoop
 */
public class ItemTid extends HashMap<String, ArrayList<LongWritable>> {

    public ItemTid() {
        super();
    }
    
    /**
     * Insere uma TID para um ITEM
     * @param key
     * @param value
     * @return 
     */
    public boolean putValue(String key, LongWritable value){
        
        if(this.containsKey(key)){
            
            ArrayList<LongWritable> v = this.get(key);
            v.add(value);
        }else{
            ArrayList<LongWritable> v = new ArrayList();
            v.add(value);
            
            this.put(key, v);
            
        }
        
        return true;
    }
    
    public static void main(String[] args) throws Exception {
        ItemTid t = new ItemTid();
        
        final ArrayList<LongWritable> tid1 = new ArrayList();
        final ArrayList<LongWritable> tid2 = new ArrayList();
        
        tid1.add(new LongWritable(2));
        tid2.add(new LongWritable(1));
        tid1.add(new LongWritable(4));
        tid2.add(new LongWritable(4));
        tid1.add(new LongWritable(5));
        tid2.add(new LongWritable(10));
        tid1.add(new LongWritable(6));
        tid2.add(new LongWritable(11));
        tid1.add(new LongWritable(11));
        tid2.add(new LongWritable(20));
        tid1.add(new LongWritable(15));
        tid2.add(new LongWritable(21));
        
//        for (int i = 0; i < 40000; i++) {
//            tid1.add(new LongWritable(i));
//        }
//        for (int i = 0; i < 40000; i++) {
//            tid2.add(new LongWritable(40000+i));
//        }
        System.out.println(calcSimi(tid1, tid2));
        System.out.println(calcSimi2(tid1, tid2));
        System.out.println(intersection(tid1, tid2));
        System.out.println(intersection2(tid1, tid2));
        
//        Evaluator.evaluator(new Callable() {
//
//                public Integer call() {
//                        ItemTid.calcSimi(tid1, tid2);
//                        return 0;
//                }
//        }, new Callable() {
//
//                public Integer call() {
//                    ItemTid.intersection2(tid1, tid2);
//                        return 0;
//                }
//        }, 100, 20);//vezes de avaliação e repetição
//        
        
    }
    
    /**
     * 
     * @param tid1
     * @param tid2
     * @return 
     */
    public static ArrayList<LongWritable> calcSimi(ArrayList<LongWritable> tid1, ArrayList<LongWritable> tid2){
        
        ArrayList<LongWritable> count = new ArrayList();
        int beginTid1 = 0;
        int beginTid2 = 0;
//        int compare;
        for_tid1:
        for (int i = beginTid1; i < tid1.size(); i++) {
            
            for_tid2:
            for (int j = beginTid2; j < tid2.size(); j++) {
                
//               if(i == 0){
                    
                    if(tid1.get(i).get() < tid2.get(j).get()){
                        
                        continue for_tid1;
                    }else if(tid1.get(i).get() > tid2.get(j).get()){
                            beginTid2 = j+1;
                            
                            continue for_tid2;
                            
                    }
//                }
                if(tid1.get(i).get() == tid2.get(j).get()){
                    count.add(tid1.get(i));
                }
                continue for_tid1;
                
            }
            
            if(beginTid2 == tid2.size()) break;
            
        }
        
        return count;
    }
    
    public static ArrayList<LongWritable> intersection(ArrayList<LongWritable> tid1, ArrayList<LongWritable> tid2) {//(lt1+lt2)*c
                int comp;
                LongWritable t1;
                LongWritable t2;
                ArrayList<LongWritable> list = new ArrayList();
                int i1 = 0;
                int i2 = 0;
                try {
                    t1 = tid1.get(i1);
                    t2 = tid2.get(i2);
                        while (true) {//lt1+lt2*(c+5)
                            comp = t1.compareTo(t2);
                          
                            if (comp > 0) {//1
                                t2 = tid2.get(++i2);
                            } else if (comp < 0) {//1
                                    t1 = tid1.get(++i1);//1
                            } else {
                                    list.add(t1);//1
                                    t1 = tid1.get(++i1);
                                    t2 = tid2.get(++i2);
                            }
                        }
                } catch (Exception e) {
                }
                return list;
        }
    public static ArrayList<LongWritable> intersection2(ArrayList<LongWritable> tid1, ArrayList<LongWritable> tid2) {//(lt1+lt2)*c
                int comp;
                LongWritable t1;
                LongWritable t2;
                ArrayList<LongWritable> list = new ArrayList();
                int i1 = 0;
                int i2 = 0;
                
                    t1 = tid1.get(i1);
                    t2 = tid2.get(i2);
                        while (true) {//lt1+lt2*(c+5)
                            comp = t1.compareTo(t2);
                          
                            if (comp > 0) {//1
                                if(++i2 == tid2.size()) break;
                                t2 = tid2.get(i2);
                            } else if (comp < 0) {//1
                                    if(++i1 == tid1.size()) break;
                                    t1 = tid1.get(i1);//1
                            } else {
                                    list.add(t1);//1
                                    if(++i2 == tid2.size()) break;
                                    if(++i1 == tid1.size()) break;
                                    t1 = tid1.get(i1);
                                    t2 = tid2.get(i2);
                            }
                        }
                
                return list;
        }
    
    /**
     * 
     * @param tid1
     * @param tid2
     * @return 
     */
    public static ArrayList<LongWritable> calcSimi2(ArrayList<LongWritable> tid1, ArrayList<LongWritable> tid2){
        
        ArrayList<LongWritable> count = new ArrayList();
       
        LongWritable v;
        for (Iterator<LongWritable> i = tid1.iterator();i.hasNext();) {
            v = i.next();
            if(tid2.contains(v)){
                count.add(v);
            }
        }
        
        return count;
    }
    
    /**
     * 
     * @param item
     * @return 
     */
    public int getItemSupport(Object item){
        
        ArrayList<LongWritable> v = this.get(item);
        
        if(v == null){
            return 0;
        }
        
        return v.size();
    }
    
}
