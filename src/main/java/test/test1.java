package test;

/**
 * Created by bigdata on 16-5-9.
 */
public class test1 {
    public static void main(String[] args) {
        String str ="2013\tall\t6\t23";
        String[] str1=str.split("\t");
        for (int i = 0; i < str1.length; i++) {
            System.out.println(str1[i]);
        }

        String [] month={"1","2","3","4","5","6"};
        StringBuffer ss=new StringBuffer();
        for (int i = 0; i < month.length; i++) {
            if (str1[2].equals(month[i])){
                ss.append(str1[0]+"\t"+str1[1]+"\t"+month[i]+"\t"+str1[3]);
            }
        }
        System.out.println(ss);

    }
}
