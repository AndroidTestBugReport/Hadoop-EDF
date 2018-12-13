package org.yyw.HadoopEDF;

import java.awt.List;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.ArrayList;

/**
 * Hello world!
 *
 */
public class App 
{
    @SuppressWarnings("deprecation")
	public static void main( String[] args )
    {
    	ArrayList<String> words = new ArrayList<String>();
        words.add("school");
        words.add(0, "at");
        String w = words.get(0);
        String x = words.get(1);
        System.out.println(w);
        System.out.println(x);
       // System.out.println((int) Math.ceil(2.1) );//保留小数凑整
       // System.out.println((int) Math.floor(2.1) );//舍掉小数取整
       // System.out.println((int) Math.ceil(2.1) );//保留小数凑整
        //System.out.println(new DecimalFormat("0").format(2.56));//四舍五入取整
       // System.out.println((double)37914/27 );
        System.out.println((int) Math.ceil((double)37914/27) );//保留小数凑整
   
       // System.out.println((int) Math.floor(37914/27) );//舍掉小数取整
       // System.out.println((int) Math.ceil(2.1) );//保留小数凑整
       // System.out.println(new DecimalFormat("0").format(37914/27));//四舍五入取整
    }
}
