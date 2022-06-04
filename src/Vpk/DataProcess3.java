package Vpk;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.*;

import static java.util.stream.Collectors.toList;

public class DataProcess3 {

    public static int testgenData(long ran_scale, int arr_scale, String tablename, Object... params) {
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
                int a1 = rand.nextInt(arr_scale);
                float a2 = DataProcess.nextFloat(0, arr_scale);
                double a3 = DataProcess.nextDouble(0, arr_scale);
                int a4 = rand.nextInt(arr_scale);
                int a5 = rand.nextInt(arr_scale);
                int a6 = rand.nextInt(arr_scale);
                float a7 = DataProcess.nextFloat(0, arr_scale);
                int a8 = rand.nextInt(arr_scale);
                float a9 = DataProcess.nextFloat(0, arr_scale);
                int a10 = rand.nextInt(arr_scale);

                pstmt.setObject(1,a1);
                pstmt.setObject(2,a2);
                pstmt.setObject(3,a3);
                pstmt.setObject(4,a4);
                pstmt.setObject(5,a5);
                pstmt.setObject(6,a6);
                pstmt.setObject(7,a7);
                pstmt.setObject(8,a8);
                pstmt.setObject(9,a9);
                pstmt.setObject(10,a10);
                pstmt.executeUpdate();
            }

        } catch(SQLException e){
            e.printStackTrace();
        } finally{
            JDBCUtil.DBClose(con,pstmt,null);
        }
        return result;
    }


    public static String testencryptMD5(String str){
        try {
            byte[] bytes=str.getBytes();
            MessageDigest messageDigest=MessageDigest.getInstance("MD5");
            messageDigest.update(bytes);
            bytes = messageDigest.digest();

            // digest()最后确定返回md5 hash值，返回值为8位字符串。因为md5 hash值是16位的hex值，实际上就是8位的字符
            // BigInteger函数则将8位的字符串转换成16位hex值，用字符串来表示；得到字符串形式的hash值
            //一个byte是八位二进制，也就是2位十六进制字符（2的8次方等于16的2次方）
            return new BigInteger(1, bytes).toString(2);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static HashMap<String, Integer> genVPK5(double[] originProb, String tablename, int tupleLimit, String sk, String[] params){
        Connection con = JDBCUtil.getConnection();
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        HashMap<String, Integer> vpk = new HashMap<>();
        int lsbSize = 3;                        // lsb的大小
        double[] prob = originProb.clone();       //动态变化的各属性选取概率
        char[][] probBin = DataProcess5.allProbHashBinChars(originProb, params, tupleLimit, tablename, sk, lsbSize);
        //float[] testProb = {0.3f};
        //char[][] probBin = probToBinByte(testProb);
        //System.out.println("比例二进制串：" + String.copyValueOf(probBin[0]));

        try{
            int len = params.length;
            // 构建sql查询语句
            String arrStr = params[0];
            for (int i = 1; i < len; i++) {
                arrStr = arrStr + "," + params[i];
            }
            String sql = "select " + arrStr + " from " + tablename + " LIMIT " + tupleLimit;
            //预编译sql语句，防止sql注入
            pstmt = con.prepareStatement(sql);
            //执行语句，用结果集接收
            rs = pstmt.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();

            int countNoTuple = 0;        //统计没有使用的元组
            HashMap<String, Integer> hashMap = new HashMap<String, Integer>();      //统计每个属性被使用的值的个数
            for(String string : params){
                hashMap.put(string, 0);
            }

            int curTupleCount = 0;        //目前对多少个元组进行了操作

            while(rs.next()){
                curTupleCount++;            //统计使用的元组数
                String conStr = "";
                Integer tempVpkRepeatNum = 0;  //当前元组构成的临时VPK在VPK集中的重复个数

                //char[][] probBin = probToBinByte(prob);

                //取当前元组的各个属性值的MSB部分，并与sk进行hash，得到各属性的hash值
                String[] tempHash = new String[len];
                char[][] charHash = new char[len][];
                for (int k = 0; k < len; k++) {
                    String arrBinStrMSB = null;        // 截取每个属性值二进制串的最大有效位 msb

                    String curArrType = metaData.getColumnTypeName(k+1);
                    String tempStr = null;
                    if (curArrType == "INT") {
                        tempStr = Integer.toBinaryString(rs.getInt(k+1));
                    }else if (curArrType == "FLOAT"){
                        int temp2 = Float.floatToIntBits(rs.getFloat(k+1));
                        tempStr = Integer.toBinaryString(temp2);
                    }else if (curArrType == "DOUBLE"){
                        long temp3 = Double.doubleToLongBits(rs.getDouble(k+1));
                        tempStr = Long.toBinaryString(temp3);
                    }

                    // 截取每个属性值二进制串的最大有效位 msb
                    int tempStrLen = tempStr.length();

                    if (tempStrLen>lsbSize){
                        arrBinStrMSB = tempStr.substring(0, tempStrLen-lsbSize);
                        tempHash[k] = testencryptMD5(sk.concat(arrBinStrMSB));
                        charHash[k] = tempHash[k].toCharArray();
                    }else {
                        tempHash[k] = "N";            // 为空
                        char[] charHashStrMSB = {'N'};
                        charHash[k] = charHashStrMSB;
                    }

                }

                List<String> tempUsedArrToVPK = new ArrayList<>();     // 记录当前元组暂时选用那些属性来构建VPK（属性名）
                for (int k = 0; k < len; k++) {
                    int prolen = probBin[k].length;    //当前元组某个属性的概率字节数组的长度
                    String arrCur = params[k];

                    // 当前属性满足要求属性就跳过
                    Integer tempArrNumUsed = hashMap.get(arrCur);
                    if (tempArrNumUsed >= (float)tupleLimit * (prob[k])){
                        continue;
                    }

                    // 该属性的MSB不足够，跳过该属性
                    if (tempHash[k]=="N"){
                        continue;
                    }

                    int charHashlen = charHash[k].length;

                    for (int i = 0; i < prolen; i++) {
                        char curCharHash = charHash[k][charHashlen-i-1];
                        if (curCharHash=='0'){
                            if (i==prolen-1 && probBin[k][i]=='0'){
                                conStr = conStr.concat(tempHash[k]);
                                tempUsedArrToVPK.add(arrCur);
                                break;
                            }
                            continue;
                        }else if (curCharHash=='1' && probBin[k][i]=='1') {
                            conStr = conStr.concat(tempHash[k]);
                            tempUsedArrToVPK.add(arrCur);
                            break;
                        }else if (curCharHash=='1' && probBin[k][i]=='0') {
                            break;
                        }
                    }

                }

                if (conStr=="") {
                    countNoTuple++;
                }
                else{
                    conStr = testencryptMD5(conStr);
                    for(String arrName : tempUsedArrToVPK){
                        int num = hashMap.get(arrName);
                        hashMap.put(arrName, num+1);
                    }
                    // 判断该元组的vpk是否重复
                    tempVpkRepeatNum = vpk.get(conStr);
                    if (tempVpkRepeatNum == null){
                        vpk.put(conStr, 1);
                    }
                    else {
                        vpk.put(conStr, tempVpkRepeatNum+1);
                    }

                }

            }
            for (String arrName : params){
                int arrCount = hashMap.get(arrName);
                System.out.println("属性" + arrName + "使用了："+ arrCount +"/"+ tupleLimit +" "+ (float)arrCount/tupleLimit);
            }
            System.out.println("没有使用的元组: " + countNoTuple);
        } catch(SQLException e){
            e.printStackTrace();
        } finally{
            JDBCUtil.DBClose(con,pstmt,null);
        }
        return vpk;
    }

    public static void arrValueToHashString(String curArrType, int lsbSize){

    }

    /**
     * 自适应调整属性的概率
     * @param adaptiveArrProb
     * @param originArrProb
     * @param tempArrNumUsed
     * @param curTupleCount
     * @return
     */
    public static float adaptiveProb(float adaptiveArrProb, float originArrProb, int tempArrNumUsed, int curTupleCount){
        float curArrProb = (float)tempArrNumUsed / curTupleCount;
        if ( originArrProb - curArrProb > 0.00001f ){
            //当前属性的选取概率不足，将概率提升
            float upchange = (1.0f - adaptiveArrProb) / 2;
            if (upchange > 1E-32){
                adaptiveArrProb = adaptiveArrProb + upchange;
            }
        }else if( curArrProb - originArrProb > 0.00001f ){
            //当前属性的选取概率过多，将概率减小
            float downchange = adaptiveArrProb / 2;
            if (downchange > 1E-32){
                adaptiveArrProb = adaptiveArrProb - downchange;
            }
        }else{
            adaptiveArrProb = originArrProb;
        }
        return adaptiveArrProb;
    }

    public static void delCompare(List<String> origin_vpk, List<String> delArr_vpk){
        List<String> intersection = delArr_vpk.stream().filter(item -> origin_vpk.contains(item)).collect(toList());        // 找两个list的交集
        int keep_number = intersection.size();     // 删除属性后，保持不变的VPK数量
        int change_number = origin_vpk.size()-keep_number;   //删除属性后，被破坏的VPK数量

        System.out.println("删除属性后，保持不变的VPK数量" + keep_number);
        System.out.println("删除属性后，被破坏的VPK数量" + change_number);
    }

    public static void valDupVPK(HashMap<String, Integer> vpk){

    }

    public static char[][] probToBinByte(float[] prob){
        char[][] probChar = new char[prob.length][];
        for (int probIndex=0; probIndex<prob.length; probIndex++){

            int n = Float.floatToIntBits(prob[probIndex]);
            String binStr = Integer.toBinaryString(n);
            String exponent = "";    //阶码
            String fraction = "";    //尾数
            int pos = 32-binStr.length();
            int subPos = 0;
            for (; pos < 9; subPos++,pos++) {}
            exponent = binStr.substring(0, subPos);
            fraction = binStr.substring(subPos);
            fraction = "1".concat(fraction);
            char[] charFrac = fraction.toCharArray();
            int intExp = 127 - Integer.parseUnsignedInt(exponent,2);

            for (int i = fraction.length()-1; i>=0; i--) {
                if (charFrac[i]=='1'){
                    fraction = fraction.substring(0, i+1);
                    break;
                }
            }

            for (int i = 0; i < intExp-1; i++) {
                fraction = "0".concat(fraction);
            }
            //System.out.println(fraction);         //打印转换后得到的二进制字符串
            probChar[probIndex] = fraction.toCharArray();
        }
        return probChar;
    }

    public static void evaluateIndicator_One(HashMap<String, Integer> vpk, int tupleLimit){
        Set<String> vpkSet = vpk.keySet();
        int exclusiveNum = 0;
        float duplicateGroupFraction = 0;
        float p = 0;
        int count = 0;
        int max = 0;
        for (String vpkstr : vpkSet){
            int temp = vpk.get(vpkstr);
            if (temp>max){
                max = temp;
            }
            if (temp==1){
                exclusiveNum++;
                //System.out.println(vpkstr);
            }else{
                count++;
                duplicateGroupFraction = duplicateGroupFraction + 1.0f / temp;
            }
        }
        p = (duplicateGroupFraction+(float)exclusiveNum) * 100 / tupleLimit;

        System.out.println("VPK排他值有" + exclusiveNum);
        System.out.println("Group数：" + count);
        System.out.println("Group中最大size为：" + max);
        System.out.println("指标p：" + p);
    }

    public static void main(String[] args) {
        /**
         * 将每个属性的使用概率转换为对应的二进制字符数组
         */
        double[] probs = {0.5f, 0.3f,
                0.2f, 0.2f, 0.3f, 0.2f, 0.4f};

        /**
         * 生成VPK，并统计VPK相关参数
         */
        String tablename = "forest";
        int tupleLimit = 30000;
        String sk = "100101000111";
        String[] arrtributes = {"Elevation", "Aspect", "Horizontal_Hydrology", "Vertical_Hydrology",           // "Slope"  "Hillshade_9am", "Hillshade_Noon"
                "Horizontal_Roadways", "Hillshade_3pm", "Horizontal_Fire_Points"};

        // 测试

        //double[] prob2 = {0.3};
        //String[] arrtributes2 = {"Elevation"};
        HashMap<String, Integer> origin_vpk = genVPK5(probs, tablename, tupleLimit, sk, arrtributes);     //在forest表中生成VPK

        evaluateIndicator_One(origin_vpk, tupleLimit);

        // 判断删除属性后VPK的保持/损坏情况
        //delCompare(origin_vpk, delArr_vpk);

    }

}

