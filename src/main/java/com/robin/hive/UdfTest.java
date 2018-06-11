package com.robin.hive;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.HashMap;

public class UdfTest extends UDF{
    private static HashMap<String,String> countryMap = new HashMap();

    static {
        countryMap.put("china", "中国");
        countryMap.put("japan", "日本");

    }

    //此段代码进行国家的转换
    public  String evaluate(String str){
        String country  = countryMap.get(str);
        if(country ==null){
            return "其他";
        }else{
            return country;
        }
    }

    //在函数中可以定义多个evaluate方法，进行重载
    //此段代码进行国家和IP的拼接，测试重载用
    public  String evaluate(String country,String ip){

        return country+"_"+ip;
    }

    /*
     *
     *此段代码用于测试上面编写的方法是否正确
    public static void main(String[] args) {
        UdfTest ut = new UdfTest();
        // TODO Auto-generated method stub
        String aa = ut.evaluate("AAAAAA");
        System.out.println(aa);
    }
    */
}
