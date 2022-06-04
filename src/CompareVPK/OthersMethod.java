package CompareVPK;

import EmbedAndExtract.EmbedAndExtract;
import Vpk.DataProcess2;
import Vpk.JDBCUtil;

import java.math.BigInteger;
import java.sql.*;
import java.util.*;

public class OthersMethod {
    public static HashMap<String, Integer> genVPK(String[] arrtributes, int[][] imgWM, int[][] EmbedWMPos, String tablename, int tupleLimitStart, int tupleLimit, int lsbSize, String sk, int msbSize){
        Connection con = JDBCUtil.getConnection();
        PreparedStatement pstmt = null;
        PreparedStatement pstmtEmbed = null;
        ResultSet rs = null;

        Arrays.sort(arrtributes);    // 根据name字母顺序排序
        HashMap<String, Integer> vpk = new HashMap<>();
        int step = 1; // 步长
        int vpkArrLimit = 3; // 每个元组构成vpk的限制
        int len = arrtributes.length;

        HashMap<String, Integer> arrUsedHashMap = new HashMap<String, Integer>();      //统计每个属性被使用的值的个数
        for(String string : arrtributes){
            arrUsedHashMap.put(string, 0);
        }

        int width = imgWM[0].length;    // 水印图片的宽度
        int height = imgWM.length;      // 水印图片的高度
        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width; j++) {
                EmbedWMPos[i][j] = 0;
            }
        }

        try{
            // 构建sql查询语句
            String arrStr = "id";
            for (int i = 0; i < len; i++) {
                arrStr = arrStr + "," + arrtributes[i];
            }
            String sql = "select " + arrStr + " from " + tablename + " where "+ tablename + ".id" +" between " + tupleLimitStart + " and " + (tupleLimitStart+tupleLimit-1);
            //预编译sql语句，防止sql注入
            pstmt = con.prepareStatement(sql);
            //执行语句，用结果集接收
            rs = pstmt.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();

            int countNoTuple = 0;        //统计没有使用的元组

            while(rs.next()){
                int tempstep = step;
                String conStr = "";
                int countAmaxArrZero = 0;

                //取当前元组的各个属性值的MSB部分，并与sk进行hash，得到各属性的hash值
                BigInteger[] tempHash = new BigInteger[len];     // 存储MSB的hash后的十进制值
                char[][] curTupleArrHashBinChar = new char[len][];    // 存储元组属性完整值hash后的二进制字符
                BigInteger curTupleMaxArr = new BigInteger("0");
                int curTupleMaxArrIndex = 0;
                BigInteger curTupleAvgArr = new BigInteger("0");
                BigInteger curTupleSumArr = new BigInteger("0");
                for (int k = 0; k < len; k++) {
                    String arrBinStrMSB = null;        // 截取每个属性值二进制串的最大有效位 msb

                    String curArrType = metaData.getColumnTypeName(k+2);
                    String tempStr = null;
                    if (curArrType == "INT") {
                        int tempint = rs.getInt(k+2);
                        tempStr = Integer.toBinaryString(tempint);
                    }else if (curArrType == "FLOAT"){
                        float tempfloat = rs.getFloat(k+2);
                        int temp2 = Float.floatToIntBits(tempfloat);
                        tempStr = Integer.toBinaryString(temp2);
                    }else if (curArrType == "DOUBLE"){
                        double tempdouble = rs.getDouble(k+2);
                        long temp3 = Double.doubleToLongBits(tempdouble);
                        tempStr = Long.toBinaryString(temp3);
                    }

                    // 截取每个属性值二进制串的最大有效位 msb
                    int tempStrLen = tempStr.length();
                    curTupleArrHashBinChar[k] = tempStr.toCharArray();

                    if (tempStrLen>lsbSize){
//                        if (tempStrLen-lsbSize < msbSize){
//                            arrBinStrMSB = tempStr.substring(0, tempStrLen-lsbSize);
//                        }else{
//                            arrBinStrMSB = tempStr.substring(0, msbSize);
//                        }
                        arrBinStrMSB = tempStr.substring(0, tempStrLen-lsbSize);
                        String decimalArrBinStrMSB = new BigInteger(arrBinStrMSB,2).toString(10);
                        String tempBinHashDecimalArrMSB = DataProcess2.testencryptMD5(decimalArrBinStrMSB.concat(sk));
                        BigInteger temp = new BigInteger(tempBinHashDecimalArrMSB,2);
                        if (temp.compareTo(curTupleMaxArr)==1){
                            curTupleMaxArr = temp;
                            curTupleMaxArrIndex = k;
                        }
                        curTupleSumArr = curTupleSumArr.add(temp);
                        tempHash[k] = temp;
                    }else {
                        tempHash[k] = new BigInteger("0");            // 为空
                    }
                }
                // 计算Amax里面的0个数
                countAmaxArrZero = countAmaxZero(curTupleMaxArr);
                // 计算Aavg平均数
                BigInteger arrNum = new BigInteger("10");
                curTupleAvgArr = curTupleSumArr.divide(arrNum);

                BigInteger tempArrHashBi = curTupleMaxArr;
                BigInteger two = new BigInteger("2");

                // Amax为偶数
                if (tempArrHashBi.mod(two).compareTo(new BigInteger("0"))==0){
                    tempstep = tempstep; // 顺时针
                }
                // Amax为奇数
                else{
                    tempstep = -tempstep; // 逆时针
                }

                String curTupleVpk = vpkBuilder(vpk, arrUsedHashMap, countAmaxArrZero,
                        curTupleMaxArrIndex, vpkArrLimit, tempstep, curTupleAvgArr, tempHash, sk, arrtributes);
                // 使用当前元组构造的vpk，进行水印的嵌入提取工作
                EmbedAndExtract.EmbedWatermark(imgWM, width, height, curTupleVpk, curTupleArrHashBinChar,
                        len, lsbSize, tablename, rs, arrtributes, con, pstmtEmbed, EmbedWMPos);
            }

            // EmbedAndExtrack.testWMpos(vpk, BigInteger.valueOf(height), BigInteger.valueOf(width));
//            int sum = 0;
//            for (int i = 0; i < height; i++) {
//                for (int j = 0; j < width; j++) {
//                    if (EmbedWMPos[i][j]>0) {
//                        sum++;
//                    }
//                }
//            }
//            System.out.println("被嵌入的图像像素有：" + sum + "/" + height*width);
            for (String arrName : arrtributes){
                int arrCount = arrUsedHashMap.get(arrName);
                System.out.println("属性" + arrName + "使用了："+ arrCount +"/"+ (tupleLimit+tupleLimitStart-1) +" "+ (float)arrCount/(tupleLimit+tupleLimitStart-1));
            }
            System.out.println("没有使用的元组: " + countNoTuple);
        } catch(SQLException e){
            e.printStackTrace();
        } finally{
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            JDBCUtil.DBClose(con,pstmt,null);
        }
        return vpk;

    }

    public static int countAmaxZero(BigInteger Amax){
        char[] Amaxchars = Amax.toString(2).toCharArray();
        int countAmaxZero = 0;
        for (char Amaxchar : Amaxchars){
            if (Amaxchar == '0'){
                countAmaxZero++;
            }
        }
        return countAmaxZero;
    }

    public static String vpkBuilder(HashMap<String, Integer> vpk, HashMap<String, Integer> arrUsedHashMap,
                                 int countAmaxArrZero, int curTupleMaxArrIndex, int vpkArrLimit, int step,
                                 BigInteger curTupleAvgArr, BigInteger[] curTupleArrMSBBi, String sk, String[] arrtributes){
        int tempArrIndex = curTupleMaxArrIndex;    // 从起点开始根据步长走，变量定位实时位置
        int countArrforVPK = 0;
        int arrsNum = curTupleArrMSBBi.length;
        StringBuffer tempConArrToVPK = new StringBuffer();
        int flag = 0;
        if (countAmaxArrZero % 2 != 0){
            flag = 1;
        }
        while(true){
            BigInteger tempArrHashMSBbi = curTupleArrMSBBi[tempArrIndex];
            // Amax二进制串里面的0个数为偶数
            if (flag==0){
                if (tempArrHashMSBbi.compareTo(curTupleAvgArr)==1){
                    String tempStr = tempArrHashMSBbi.toString();
                    tempConArrToVPK.append(tempStr);
                    String curArrName = arrtributes[tempArrIndex];
                    int num = arrUsedHashMap.get(curArrName);
                    arrUsedHashMap.put(curArrName, num+1);
                    countArrforVPK++;
                }
            }else{
                if (tempArrHashMSBbi.compareTo(curTupleAvgArr)==-1){
                    String tempStr = tempArrHashMSBbi.toString();
                    tempConArrToVPK.append(tempStr);
                    String curArrName = arrtributes[tempArrIndex];
                    int num = arrUsedHashMap.get(curArrName);
                    arrUsedHashMap.put(curArrName, num+1);
                    countArrforVPK++;
                }
            }

            if (countArrforVPK == vpkArrLimit){
                break;
            }

            tempArrIndex = tempArrIndex + step;
            if (tempArrIndex > arrsNum-1){
                tempArrIndex = tempArrIndex - arrsNum;
            }else if (tempArrIndex < 0){
                tempArrIndex = tempArrIndex + arrsNum;
            }

            // 查看是否回到起点，且被选中的属性为0，为0，该元组浪费
            if (tempArrIndex == curTupleMaxArrIndex){
                if (countArrforVPK == 0){
                    return null;
                }
                break;
            }
        }

        String conStr = tempConArrToVPK.toString();
        // conStr = DataProcess2.testencryptMD5(sk.concat(conStr));
        //判断该元组的vpk是否重复
        // String decimalStr = new BigInteger(conStr,2).toString(10);
        Integer tempVpkRepeatNum = vpk.get(conStr);
        if (tempVpkRepeatNum == null){
            vpk.put(conStr, 1);
        }else{
            vpk.put(conStr, tempVpkRepeatNum+1);
        }

        return conStr;
    }

}
