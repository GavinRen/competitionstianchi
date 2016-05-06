package test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by bigdata on 16-4-19.
 */
public class TestCalendar {

    private static SimpleDateFormat date_format = new SimpleDateFormat("yyyyMMdd");

    public static void main(String[] args) throws Exception {
        float a= 100;
        int b=3;
        System.out.println(a/b);
        String line ="95    4   28  10";
        String [] rec =line.split(" +");
        /*if(rec[1]!=null){
            int tag =Integer.parseInt(rec[1]);
            System.out.println(tag);
        }*/
        for (int i = 0; i < rec.length; i++) {
            String s=rec[i];
            int tag=Integer.parseInt(s.trim());
            System.out.println(s);
        }
/*
        Date date = date_format.parse("20141010");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        System.out.println(calendar.get(Calendar.DAY_OF_YEAR));
        //int lable=TestCalendar.setlable("20151010");
        //System.out.println(lable);*/
    }

   /* public static int setlable(String date_str) throws ParseException {
        Date date = date_format.parse(date_str);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        if (calendar.get(Calendar.YEAR) == 2014) {
            int days = calendar.get(Calendar.DAY_OF_YEAR) - 283;
            int lable = days / 14 + 1;
            return lable;
        } else {
            if (calendar.get(Calendar.YEAR) == 2015) {
                int days = calendar.get(Calendar.DAY_OF_YEAR) + 82;
                int lable = days / 14 + 1;
                return lable;
            } else {
                return 0;
            }

        }

    }*/

}
