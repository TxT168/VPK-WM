package Vpk;

import java.sql.*;

public class JDBCUtil {
    //JDBC驱动名称
    public static String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    //链接地址，设置编码可用且为utf-8
    public static String URL="jdbc:mysql://localhost:3306/test?characterEncoding=utf-8&rewriteBatchedStatement=true";
    //数据库用户名
    public static String USER="root";
    //数据库密码
    public static String PWD="12345678";

    /**
     * 获取数据库的连接
     */
    public static Connection getConnection(){
        Connection con = null;
        try {
            //加载驱动
            Class.forName(JDBC_DRIVER);
            //创建链接
            con= DriverManager.getConnection(URL, USER, PWD);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        //返回连接
        return con;
    }

    /**
     * 数据库关闭
     */
    public static void DBClose(Connection con, PreparedStatement pstm, ResultSet rs){       //7.关闭连接，释放资源
        try {
            if (rs != null) {
                rs.close();
            }
            if (pstm != null) {
                pstm.close();
            }
            if (con != null) {
                con.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
