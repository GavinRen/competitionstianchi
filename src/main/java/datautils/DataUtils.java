package datautils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by bigdata on 16-4-19.
 */
public class DataUtils {
    private static SimpleDateFormat date_format = new SimpleDateFormat("yyyyMMdd");
    private String itemID;
    private String date;
    private String saleNumber;
    private String warehouseCode;
    public static DataUtils processData(String line){
        DataUtils cleandata = new DataUtils();
        String[]rec =line.trim().split(",");
        if (rec.length==31){
            cleandata.itemID=rec[1];
            cleandata.date=rec[0];
            cleandata.saleNumber=rec[29];
            cleandata.warehouseCode="all";
            return cleandata;
        }else{
            if (rec.length==32) {
                cleandata.itemID = rec[1];
                cleandata.date = rec[0];
                cleandata.saleNumber = rec[30];
                cleandata.warehouseCode = rec[2];
                return cleandata;
            }else{
                return cleandata;
            }
        }
    }
    public static int setlable(String date_str) throws ParseException {
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

    }

    public String getDate() {
        return date;
    }

    public String getItemID() {
        return itemID;
    }

    public String getSaleNumber() {
        return saleNumber;
    }

    public String getWarehouseCode() {
        return warehouseCode;
    }
}
