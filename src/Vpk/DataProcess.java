package Vpk;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.*;

public class DataProcess {
    private static DBDao dbDao = new DBDao();

    /**
     * 生成 min 到 max 范围的随机数
     * @param min
     * @param max
     * @return double
     */
    public static double nextDouble(final double min, final double max) {
        double rannum = min + ((max - min) * new Random().nextDouble());
        BigDecimal bd = new BigDecimal(rannum);
        double num = bd.setScale(8, RoundingMode.HALF_EVEN).doubleValue();
        return num;
    }

    /**
     * 生成 min 到 max 范围的随机数
     * @param min
     * @param max
     * @return float
     */
    public static float nextFloat(final float min, final float max) {
        return min + ((max - min) * new Random().nextFloat());
    }

    /**
     * 数据库随机生成数据
     */
    public static int genData(long ran_scale, int arr_scale, String tablename, Object... params) {
        String sql = null;

        //创建连接
        Connection con = JDBCUtil.getConnection();
        PreparedStatement pstmt = null;
        int result = 0;
        try{
            // 自增主键重新从1开始
            sql = "ALTER TABLE " + tablename + " AUTO_INCREMENT = 1";
            pstmt = con.prepareStatement(sql);
            pstmt.executeUpdate(sql);

            // 构建sql查询语句
            String arrStr = (String) params[0];
            String arrQusPos = "?";
            int len = params.length;
            for (int i = 1; i < len; i++) {
                arrStr = arrStr + "," + (String)params[i];
                arrQusPos = arrQusPos + ",?";
            }
            sql = "insert into " + tablename + " (" + arrStr + ") values(" + arrQusPos + ")";
            //预编译sql语句，防止sql注入
            pstmt = con.prepareStatement(sql);
            //开始添加随机数据
            Random rand = new Random();
            for (int i = 0; i < ran_scale; i++) {
                int name = rand.nextInt(arr_scale);
                int price = rand.nextInt(arr_scale);
                int store = rand.nextInt(arr_scale);
                float uuida = nextFloat(0, arr_scale);
                double uuidb = nextDouble(0, arr_scale);
                int uc = rand.nextInt(arr_scale);
                float ud = nextFloat(0, arr_scale);
                int ue = rand.nextInt(arr_scale);
                int uf = rand.nextInt(arr_scale);
                float ug = nextFloat(0, arr_scale);
                pstmt.setObject(1,name);
                pstmt.setObject(2,price);
                pstmt.setObject(3,store);
                pstmt.setObject(4,uuida);
                pstmt.setObject(5,uuidb);
                pstmt.setObject(6,uc);
                pstmt.setObject(7,ud);
                pstmt.setObject(8,ue);
                pstmt.setObject(9,uf);
                pstmt.setObject(10,ug);
                pstmt.executeUpdate();
            }

        } catch(SQLException e){
            e.printStackTrace();
        } finally{
            JDBCUtil.DBClose(con,pstmt,null);
        }
        return result;
    }

    /**
     * 每个属性值做密钥hash，然后通过 mod n 来控制每个属性参与构建VPK的比例
     * @param tablename
     * @param params
     * @return List<String>
     */
    public static List<String> genVPK2(String tablename, Object... params){
        Connection con = JDBCUtil.getConnection();
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        List<String> vpk = new ArrayList<>();
        String sk = "100101000111";

        try{
            int len = params.length;
            int count=0;
            String sql = "select * from " + tablename;
            //预编译sql语句，防止sql注入
            pstmt = con.prepareStatement(sql);
            //执行语句，用结果集接收
            rs = pstmt.executeQuery();
            while(rs.next()){
                String conStr = "";
                for (int j = 2; j < len+2; j++) {
                    if (rs.getInt(j) % 5 == 0) {
                        conStr = conStr.concat(Integer.toBinaryString(rs.getInt(j)));
                    }
                }
                if (conStr != ""){
                    System.out.println("VPK:" + conStr);
                    conStr = conStr.concat(sk);
                    vpk.add(encryptMD5(conStr));
                }else{
                    count++;
                }
            }
            System.out.println("浪费的元组个数：" + count);
        } catch(SQLException e){
            e.printStackTrace();
        } finally{
            JDBCUtil.DBClose(con,pstmt,null);
        }

        return vpk;
    }

    public static List<String> genVPK3(int arrNum_VPK, String tablename, Object... params){
        Connection con = JDBCUtil.getConnection();
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        List<String> vpk = new ArrayList<>();
        String sk = "100101000111";


        try{
            int len = params.length;
            int arrPos = 0;
            // 构建sql查询语句
            String arrStr = (String) params[0];
            for (int i = 1; i < len; i++) {
                arrStr = arrStr + "," + (String)params[i];
            }
            String sql = "select " + arrStr + " from " + tablename;
            //预编译sql语句，防止sql注入
            pstmt = con.prepareStatement(sql);
            //执行语句，用结果集接收
            rs = pstmt.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            long count1 = 0;
            long count2 = 0;
            long count3 = 0;

            while(rs.next()){
                String conStr = "";
                BigInteger[] arrHash = new BigInteger[arrNum_VPK];
                arrPos = arrPos % len + 1;
                int arrRunPos = arrPos;
                for (int k=0 ; k<arrNum_VPK; k++){
                    String curArrType = metaData.getColumnTypeName(arrRunPos);

                    if (curArrType == "INT"){
                        String tempStr1 = Integer.toBinaryString(rs.getInt(arrRunPos));
                        String tempHash1 = encryptMD5(sk.concat(tempStr1));
                        arrHash[k] = new BigInteger(tempHash1);
                        count1++;
                    }else if (curArrType == "FLOAT"){
                        int temp2 = Float.floatToIntBits(rs.getFloat(arrRunPos));
                        String tempStr2 = Integer.toBinaryString(temp2);
                        String tempHash2 = encryptMD5(sk.concat(tempStr2));
                        arrHash[k] = new BigInteger(tempHash2);
                        count2++;
                    }else{
                        long temp3 = Double.doubleToLongBits(rs.getDouble(arrRunPos));
                        String tempStr3 = Long.toBinaryString(temp3);
                        String tempHash3 = encryptMD5(sk.concat(tempStr3));
                        arrHash[k] = new BigInteger(tempHash3);
                        count3++;
                    }
                    arrRunPos = arrRunPos % len + 1;
                }

                Arrays.sort(arrHash);
                for (int m = 0; m < arrNum_VPK; m++) {
                    conStr = conStr + arrHash[m];
                }
                vpk.add(conStr);
            }
            System.out.println("int类型使用了："+count1);
            System.out.println("float类型使用了："+count2);
            System.out.println("double类型使用了："+count3);
        } catch(SQLException e){
            e.printStackTrace();
        } finally{
            JDBCUtil.DBClose(con,pstmt,null);
        }

        return vpk;
    }

    /**
     * 对字符串进行MD5加密
     * @param str
     * @return String
     */
    public static String encryptMD5(String str){
        try {
            byte[] bytes=str.getBytes();
            MessageDigest messageDigest=MessageDigest.getInstance("MD5");
            messageDigest.update(bytes);
            bytes = messageDigest.digest();

            // digest()最后确定返回md5 hash值，返回值为8位字符串。因为md5 hash值是16位的hex值，实际上就是8位的字符
            // BigInteger函数则将8位的字符串转换成16位hex值，用字符串来表示；得到字符串形式的hash值
            //一个byte是八位二进制，也就是2位十六进制字符（2的8次方等于16的2次方）
            return new BigInteger(1, bytes).toString(10);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void test(){
        float f = -20.3f;
        int i = Float.floatToIntBits(f);
        System.out.println(Integer.toBinaryString(i));

        int n = 2182;
        String m = Integer.toBinaryString(n);
        System.out.println(m);
        System.out.println("MD5编码结果：" + encryptMD5(m));
    }

    public static void valDupVPK(List<String> vpk){
        //List<String> vpk = genVPK2("book", "name","price","store","uuida","uuidb");
        HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
        int i = 0;
        int count = 0;
        for (String string : vpk) {
            i++;
            if (hashMap.get(string) != null) {
                //System.out.println(i);
                count++;
                Integer value = hashMap.get(string);
                hashMap.put(string, value+1);
                //System.out.println("the element:"+string+" is repeat");
            } else {
                hashMap.put(string, 1);
                //System.out.println(string);
            }
        }
        System.out.println("重复VPK：" + count);
    }

//    public static void main(String[] args) {
//        //生成随机数据
//        genData();
//
//        //genVPK2("book","name","price", "store","uuida","uuidb");
//        //valDupVPK();
//
//        List<String> vpk = genVPK3("book","name","price", "store","uuida","uuidb", "uc", "ud", "ue", "uf", "ug");
//        valDupVPK(vpk);
//    }
}
