/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.app;

import java.util.ArrayList;

/**
 *
 * @author eduardo
 */
public class StructTeste {
    ArrayList<String> key;
    ArrayList<String> value;

    public StructTeste() {
        key = new ArrayList();
        value = new ArrayList();
    }
    
    public void put(String k, String v){
        key.add(k);
        value.add(v);
    }
    
    public int indexOF(String k){
        return key.indexOf(k);
    }
    public String get(String k){
        int i = key.indexOf(k);
        return value.get(i);
    }
}
