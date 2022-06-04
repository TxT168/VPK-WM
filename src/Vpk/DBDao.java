package Vpk;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class DBDao {

    /**
     * Task:执行添加、更新、删除操作，返回数据库受到影响的行数
     * @params String sql
     * @params Object params
     * @return int result
     */
    public static int executeUpdate(String sql, Object... params){
        //创建连接
        Connection con = JDBCUtil.getConnection();
        PreparedStatement pstmt = null;
        int result = 0;
        try{
            //预编译sql语句，防止sql注入
            pstmt = con.prepareStatement(sql);
            //传递参数，如果参数存在
            if (params!=null) {
                //循环传参
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i+1, params[i]);
                }
            }
            //执行sql语句，返回受影响行数
            result = pstmt.executeUpdate();
        } catch(SQLException e){
            e.printStackTrace();
        } finally{
            JDBCUtil.DBClose(con,pstmt,null);
        }
        return result;
    }

    /**
     *进行数据的查询，通过自建泛型RowMap，进行数据类型的定义
     *@params String sql
     *@params Object params
     *@return list<Integer>
     */
    public static ResultSet executeQuery(String sql, Object... params){
        //创建连接
        Connection con = JDBCUtil.getConnection();
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            //绑定sql语句
            pstmt = con.prepareStatement(sql);
            //循环传参
            if(params != null){
                for(int i=0; i<params.length; i++){
                    pstmt.setObject(i+1, params[i]);
                }
            }
            //执行语句，用结果集接收
            rs=pstmt.executeQuery();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally{
            return rs;
        }
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

    public static List<String> genVPK(String tablename, Object... params){
        String sql = "SELECT b2.number, b1."+ params[0] +", b1."+ params[1] +", b1."+ params[2] +" FROM " + tablename +" b1 " +
                "LEFT JOIN ( SELECT "+ params[0] +", COUNT("+ params[0] +") as number from "+ tablename +" " +
                "GROUP BY "+ params[0] +" HAVING COUNT("+ params[0] +")>=1 " +
                "ORDER BY number) b2 ON b1."+ params[0] +" = b2."+ params[0] +" " +
                "ORDER BY b2.number, b1."+ params[0] +", b1."+ params[1] +", b1." + params[2];
        List<String> vpk = new ArrayList<>();
        String sk = "100101000111";

        Connection con = JDBCUtil.getConnection();
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try{
            //预编译sql语句，防止sql注入
            pstmt = con.prepareStatement(sql);
            //执行sql语句，返回受影响行数
            //执行语句，用结果集接收
            rs=pstmt.executeQuery();
            while(rs.next()){
                //利用自建泛型实现数组的添加
                int duplicate = rs.getInt(1);
                String binA = Integer.toBinaryString(rs.getInt(2));
                String conStr = null;
                if ( duplicate > 1 ) {
                    String binB = Integer.toBinaryString(rs.getInt(3));
                    conStr = sk.concat(binA);
                    conStr = conStr.concat(binB);
                }else{
                    conStr = sk.concat(binA);
                }
                vpk.add(encryptMD5(conStr));
            }

        } catch(SQLException e){
            e.printStackTrace();
        } finally{
            JDBCUtil.DBClose(con,pstmt,null);
        }
        return vpk;
    }

    public static void main(String[] args) {
        List<String> vpk = genVPK("book", "name","price","store");
        HashMap<String, Integer> hashMap = new HashMap<String, Integer>();
        int i = 0;
        int count = 0;
        for (String string : vpk) {
            i++;
            if (hashMap.get(string) != null) {
                System.out.println(i);
                count++;
                Integer value = hashMap.get(string);
                hashMap.put(string, value+1);
                System.out.println("the element:"+string+" is repeat");
            } else {
                hashMap.put(string, 1);
            }
        }
        System.out.println("重复VPK：" + count);
    }


}
