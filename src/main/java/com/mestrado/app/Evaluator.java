/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package main.java.com.mestrado.app;

/**
 *
 * @author eduardo
 */
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.concurrent.Callable;

public class Evaluator implements Serializable{
     /**
     *
     * @param callA metodo A
     * @param callB metodo B
     * @param forTimes quantas vezes cada metodo deve ser chamado
     * @param timesEvaluate quantas vezes a avaliacao deve ser efetuada para calcular a media
     * @throws Exception
     */
        public  static void evaluator(Callable callA,Callable callB, int forTimes, int timesEvaluate) throws Exception {


                double a[] = new double[timesEvaluate];
                double b[] = new double[timesEvaluate];
                for (int vezes = 0; vezes < timesEvaluate; ++vezes) {
                        double ini;
                        ini = System.currentTimeMillis();
                        //BEGIN THE CODE A TO EVALUATOR
                        for (int i = 0; i < forTimes; ++i) {
                                callA.call();
                        }
                        //END CODE A TO EVALUATOR
                        a[vezes] = (System.currentTimeMillis() - ini) / 1000.0;
                        System.out.println("Time a: " + a[vezes]);
                        ini = System.currentTimeMillis();

                        //BEGIN THE CODE B TO EVALUATOR
                        for (int i = 0; i < forTimes; ++i) {
                                callB.call();
                        }
                        //END CODE A TO EVALUATOR



                        b[vezes] = (System.currentTimeMillis() - ini) / 1000.0;
                        System.out.println("Time b: " + b[vezes]);
                        System.out.println();



                }
                double totala = 0;
                double totalb = 0;
                for (int i = 0; i < timesEvaluate; ++i) {
                        totala += a[i];
                        totalb += b[i];
                }
                double mediaa = totala / timesEvaluate;
                double mediab = totalb / timesEvaluate;
                System.out.println("Tempo medio para A: " + mediaa);
                System.out.println("Tempo medio para B: " + mediab);
                DecimalFormat df = new DecimalFormat("####.####");
                if (mediaa < mediab) {
                        System.out.println("A é mais rápido que B " + df.format(mediab / mediaa) + " vezes");
                }
                if (mediab < mediaa) {
                        System.out.println("B é mais rápido que A " + df.format(mediaa / mediab) + " vezes");
                }



        }
        /**
         * How to
         * @param args no args
         * @throws Exception
         */
        public static void main(String [] args) throws Exception{
           String b = "a";
           for(int i=0; i < 10; ++i){
               b+=b;
           }
           System.out.println("ko");
           final String d = b;

                Evaluator.evaluator(new Callable() {

                        public Integer call() {
                                StringBuilder sb = new StringBuilder();
                                for(int i=0; i < 10000;++i){
                                    sb.append(d+d+d+d);
                                }
                                return 0;
                        }
                }, new Callable() {

                        public Integer call() {
                                StringBuilder sb = new StringBuilder();
                                for(int i=0; i < 10000;++i){
                                    sb.append(d).append(d).append(d).append(d);
                                }
                                return 0;
                        }
                }, 30, 10);
        }
}

