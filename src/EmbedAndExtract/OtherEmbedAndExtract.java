package EmbedAndExtract;

import Vpk.DataProcess2;
import Vpk.DataProcess5;
import Vpk.JDBCUtil;

import java.math.BigInteger;
import java.sql.*;
import java.util.*;

public class OtherEmbedAndExtract {
    public static void EmbedWatermark(int[][] wm, int width, int height, String vpk, char[][] curTupleArrBinChar, int arrNum, int lsbSize,
                                      String tablename, ResultSet rs, String[] arrtributes, Connection con, PreparedStatement pstmtEmbed, int[][] EmbedWMPos) throws SQLException {
        int pk = rs.getInt(1);
        BigInteger biVpk = new BigInteger(vpk);
        int selectArrIndex = biVpk.mod(BigInteger.valueOf(arrNum)).intValue();
        char[] selectArrBinChars = null;
        int selectArrBinLen = 0;
        for (int i=0; i<arrNum; i++){
            selectArrBinChars = curTupleArrBinChar[selectArrIndex];
            selectArrBinLen = selectArrBinChars.length;
            if (selectArrBinLen > 2 * lsbSize){
                break;
            }
            selectArrIndex = (selectArrIndex+1) % arrNum;
        }

        int selectLsbIndex = selectArrBinLen - biVpk.mod(BigInteger.valueOf(lsbSize)).intValue() - 1;

        String hashVpk = DataProcess2.testencryptMD5(vpk.concat("1"));
        String hashVpk2 = DataProcess2.testencryptMD5(vpk.concat("2"));
        String hashVpkTen = new BigInteger(hashVpk,2).toString(10);
        String hashVpkTen2 = new BigInteger(hashVpk2,2).toString(10);
        BigInteger newHashVpkNum = new BigInteger(hashVpkTen);
        BigInteger newHashVpkNum2 = new BigInteger(hashVpkTen2);
        int hPos = newHashVpkNum.mod(BigInteger.valueOf(height)).intValue();
        int wPos = newHashVpkNum2.mod(BigInteger.valueOf(width)).intValue();
        EmbedWMPos[hPos][wPos] = EmbedWMPos[hPos][wPos] + 1;

        int selectImgPixel = wm[hPos][wPos];
        int msbLen = selectArrBinLen - lsbSize;
        int selectMsbIndex = biVpk.mod(BigInteger.valueOf(msbLen)).intValue();
        char selectArrBinMsbChar = selectArrBinChars[selectMsbIndex];
        int selectArrBinMsb = 0;
        if (selectArrBinMsbChar=='1'){
            selectArrBinMsb = 1;
        }
        int msbXorImagepixel = selectArrBinMsb ^ selectImgPixel;
        if (msbXorImagepixel==1){
            selectArrBinChars[selectLsbIndex] = '1';
        }else{
            selectArrBinChars[selectLsbIndex] = '0';
        }
        // 嵌入水印后的属性值字符串
        String selectArrNewBinStr = String.copyValueOf(selectArrBinChars);
        Object wmArrVal = 0;

        ResultSetMetaData metaData = rs.getMetaData();
        String selectArrType = metaData.getColumnTypeName(selectArrIndex+2);
        if (selectArrType == "INT") {
            BigInteger bi = new BigInteger(selectArrNewBinStr, 2);
            wmArrVal = bi.intValue();
        }else if (selectArrType == "FLOAT"){
            int temp = Integer.parseInt(selectArrNewBinStr, 2);
            wmArrVal = Float.intBitsToFloat(temp);
        }else if (selectArrType == "DOUBLE"){
            long temp = Long.parseLong(selectArrNewBinStr, 2);
            wmArrVal = Double.longBitsToDouble(temp);
        }

        // 开始更新数据库
        // 构建sql
        String sql = "UPDATE "+ tablename +" SET ";
        // System.out.println("选中属性："+selectArrIndex);
        sql = sql + arrtributes[selectArrIndex] + "=" + wmArrVal + " WHERE id = " + pk;
        try{
            pstmtEmbed = con.prepareStatement(sql);
            //System.out.println("更新的语句：" + sql);
            pstmtEmbed.executeUpdate(sql);
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    public static void ExtractPixelWatermark(int[][] ExtractWM, int width, int height, String vpk,
                                             HashMap<String, char[]> curTupleArrBinChar, int originArrNum,
                                             int lsbSize, String[] arrtributes, String[] delArrs){

        BigInteger biVpk = new BigInteger(vpk);
        int selectArrIndex = biVpk.mod(BigInteger.valueOf(originArrNum)).intValue();
        char[] selectArrBinChars = null;
        int selectArrBinLen = 0;
        for (int i=0; i<originArrNum ;i++){
            String selectArr = arrtributes[selectArrIndex];
            for (String delArr : delArrs){
                if (selectArr == delArr)
                    return;
            }
            selectArrBinChars = curTupleArrBinChar.get(selectArr);
            selectArrBinLen = selectArrBinChars.length;
            if (selectArrBinLen > 2 * lsbSize){
                break;
            }
            selectArrIndex = (selectArrIndex+1) % originArrNum;
        }

        int selectLsbIndex = selectArrBinLen - biVpk.mod(BigInteger.valueOf(lsbSize)).intValue() -1;
        //char selectArrBinLsbChar = selectArrBinChars[selectLsbIndex];

        int msbLen = selectArrBinLen - lsbSize;
        int selectMsbIndex = biVpk.mod(BigInteger.valueOf(msbLen)).intValue();
        char selectArrBinMsbChar = selectArrBinChars[selectMsbIndex];
        int ExtractPixel = selectArrBinMsbChar ^ selectArrBinChars[selectLsbIndex];

        String hashVpk = DataProcess2.testencryptMD5(vpk.concat("1"));
        String hashVpk2 = DataProcess2.testencryptMD5(vpk.concat("2"));
        String hashVpkTen = new BigInteger(hashVpk,2).toString(10);
        String hashVpkTen2 = new BigInteger(hashVpk2,2).toString(10);
        BigInteger newHashVpkNum = new BigInteger(hashVpkTen);
        BigInteger newHashVpkNum2 = new BigInteger(hashVpkTen2);
        int hPos = newHashVpkNum.mod(BigInteger.valueOf(height)).intValue();
        int wPos = newHashVpkNum2.mod(BigInteger.valueOf(width)).intValue();
        // 白色 1 黑色 0
        if (ExtractPixel == 1){
            ExtractWM[hPos][wPos] = ExtractWM[hPos][wPos] + 1;
        }else {
            ExtractWM[hPos][wPos] = ExtractWM[hPos][wPos] - 1;
        }

    }

    public static int[][] ExtractWM(int width, int height, int lsbSize, String tablename,
                                    String[] arrtributes, int tupleLimitStart, int tupleLimit,
                                    String sk, String[] delArrs, int remainTupleNum, int msbSize){

        int[][] ExtractWM = new int[height][width];
        // 初始化提取的水印二维数组
        for(int i = 0; i < height; i++){
            for(int j = 0; j < width; j++){
                ExtractWM[i][j] = 0;
            }
        }

        Connection con = JDBCUtil.getConnection();
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        HashMap<String, Integer> vpk = new HashMap<>();
        int step = 1; // 步长
        int vpkArrLimit = 3; // 每个元组构成vpk的限制
        int originArrsNum = arrtributes.length;
        Arrays.sort(arrtributes);
        String[] finalArrtributes = getFinalArrtributes(arrtributes, delArrs);
        Arrays.sort(finalArrtributes);    // 根据name字母顺序排序

        int len = finalArrtributes.length;

        int[] delRemainTupleIndex = reservoirSample(tupleLimit, remainTupleNum);

        HashMap<String, Integer> arrUsedHashMap = new HashMap<String, Integer>();      //统计每个属性被使用的值的个数
        for (String string : arrtributes) {
            arrUsedHashMap.put(string, 0);
        }
        int countNoTuple = 0;        //统计没有使用的元组

        try{
            for (int tupleIndex : delRemainTupleIndex) {
                // 构建SQL查询语句
                String arrStr = "id";
                for (int i = 0; i < len; i++) {
                    arrStr = arrStr + "," + finalArrtributes[i];
                }
                // String sql = "select " + arrStr + " from " + tablename + " where "+ tablename + ".id" +" between 0 and " + tupleLimit;
                String sql = "select " + arrStr + " from " + tablename + " where " + tablename + ".id=" + tupleIndex;
                //预编译sql语句，防止sql注入
                pstmt = con.prepareStatement(sql);
                //执行语句，用结果集接收
                rs = pstmt.executeQuery();
                ResultSetMetaData metaData = rs.getMetaData();

                while (rs.next()) {
                    int tempstep = step;
                    String conStr = "";
                    int countAmaxArrZero = 0;

                    //取当前元组的各个属性值的MSB部分，并与sk进行hash，得到各属性的hash值
                    BigInteger[] tempHash = new BigInteger[len];     // 存储MSB的hash后的十进制值
                    HashMap<String, char[]> curTupleArrHashBinChar = new HashMap<>();    // 存储元组属性完整值hash后的二进制字符
                    BigInteger curTupleMaxArr = new BigInteger("0");
                    int curTupleMaxArrIndex = 0;
                    BigInteger curTupleAvgArr = new BigInteger("0");
                    BigInteger curTupleSumArr = new BigInteger("0");
                    for (int k = 0; k < len; k++) {
                        String arrBinStrMSB = null;        // 截取每个属性值二进制串的最大有效位 msb

                        String curArrType = metaData.getColumnTypeName(k + 2);
                        String tempStr = null;
                        if (curArrType == "INT") {
                            int tempint = rs.getInt(k + 2);
                            tempStr = Integer.toBinaryString(tempint);
                        } else if (curArrType == "FLOAT") {
                            float tempfloat = rs.getFloat(k + 2);
                            int temp2 = Float.floatToIntBits(tempfloat);
                            tempStr = Integer.toBinaryString(temp2);
                        } else if (curArrType == "DOUBLE") {
                            double tempdouble = rs.getDouble(k + 2);
                            long temp3 = Double.doubleToLongBits(tempdouble);
                            tempStr = Long.toBinaryString(temp3);
                        }

                        // 截取每个属性值二进制串的最大有效位 msb
                        int tempStrLen = tempStr.length();
                        curTupleArrHashBinChar.put(finalArrtributes[k],tempStr.toCharArray());

                        if (tempStrLen > lsbSize) {
//                            if (tempStrLen - lsbSize < msbSize) {
//                                arrBinStrMSB = tempStr.substring(0, tempStrLen - lsbSize);
//                            } else {
//                                arrBinStrMSB = tempStr.substring(0, msbSize);
//                            }
                            arrBinStrMSB = tempStr.substring(0, tempStrLen-lsbSize);
                            String decimalArrBinStrMSB = new BigInteger(arrBinStrMSB, 2).toString(10);
                            String tempBinHashDecimalArrMSB = DataProcess2.testencryptMD5(decimalArrBinStrMSB.concat(sk));
                            BigInteger temp = new BigInteger(tempBinHashDecimalArrMSB, 2);
                            if (temp.compareTo(curTupleMaxArr) == 1) {
                                curTupleMaxArr = temp;
                                curTupleMaxArrIndex = k;
                            }
                            curTupleSumArr = curTupleSumArr.add(temp);
                            tempHash[k] = temp;
                        } else {
                            tempHash[k] = new BigInteger("0");            // 为空
                        }
                    }
                    // 计算Amax里面的0个数
                    countAmaxArrZero = countAmaxZero(curTupleMaxArr);
                    // 计算Aavg平均数
                    BigInteger arrNum = new BigInteger(String.valueOf(len));
                    curTupleAvgArr = curTupleSumArr.divide(arrNum);

//                    int AmaxFlag = 0;
//                    for (int i=0;i<delArrs.length;i++){
//                        if (delArrs[i]==arrtributes[curTupleMaxArrIndex])
//                            AmaxFlag=1;
//                    }
//                    if (AmaxFlag==1) continue;

                    BigInteger tempArrHashBi = curTupleMaxArr;
                    BigInteger two = new BigInteger("2");

                    // Amax为偶数
                    if (tempArrHashBi.mod(two).compareTo(new BigInteger("0")) == 0) {
                        tempstep = tempstep; // 顺时针
                    }
                    // Amax为奇数
                    else {
                        tempstep = -tempstep; // 逆时针
                    }

                    String curTupleVpk = vpkBuilder(vpk, arrUsedHashMap, countAmaxArrZero,
                            curTupleMaxArrIndex, vpkArrLimit, tempstep, curTupleAvgArr, tempHash, finalArrtributes, delArrs);
                    // 使用当前元组构造的vpk，进行水印的嵌入提取工作
                    if (curTupleVpk!=null){
                        ExtractPixelWatermark(ExtractWM, width, height, curTupleVpk, curTupleArrHashBinChar,
                                originArrsNum, lsbSize, arrtributes, delArrs);
                    }else{
                        countNoTuple++;
                    }
                }
            }
            System.out.println("没有使用的元组：" + countNoTuple);
            for (String arrName : finalArrtributes){
                int arrCount = arrUsedHashMap.get(arrName);
                System.out.println("属性" + arrName + "使用了："+ arrCount +"/"+ (tupleLimit+tupleLimitStart-1) +" "+ (float)arrCount/(tupleLimit+tupleLimitStart-1));
            }
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

        for(int i = 0; i < height; i++){
            for(int j = 0; j < width; j++){
                if (ExtractWM[i][j] >= 0){
                    ExtractWM[i][j] = -1;
                }else{
                    ExtractWM[i][j] = -16777216;
                }
            }
        }
        return ExtractWM;
    }

    public static String vpkBuilder(HashMap<String, Integer> vpk, HashMap<String, Integer> arrUsedHashMap,
                                    int countAmaxArrZero, int curTupleMaxArrIndex, int vpkArrLimit, int step,
                                    BigInteger curTupleAvgArr, BigInteger[] curTupleArrMSBBi, String[] arrtributes,
                                    String[] delArrs){
        int tempArrIndex = curTupleMaxArrIndex;    // 从起点开始根据步长走，变量定位实时位置
        int countArrforVPK = 0;
        int arrsNum = curTupleArrMSBBi.length;
        StringBuffer tempConArrToVPK = new StringBuffer();
        int flag = 0;
        if (countAmaxArrZero % 2 != 0){
            flag = 1;
        }
        while(true){
            int delFlag = 0;
            BigInteger tempArrHashMSBbi = curTupleArrMSBBi[tempArrIndex];
            // Amax二进制串里面的0个数为偶数
            if (flag==0){
                if (tempArrHashMSBbi.compareTo(curTupleAvgArr)==1){
                    String tempStr = tempArrHashMSBbi.toString();
                    tempConArrToVPK.append(tempStr);
                    String curArrName = arrtributes[tempArrIndex];
//                    // 查看属性是否已经被删除，删除则该属性不能构建
//                    for (int i=0;i<delArrs.length;i++){
//                        if (delArrs[i]==curArrName)
//                            delFlag=1;
//                    }
//                    if (delFlag==0){
//                        int num = arrUsedHashMap.get(curArrName);
//                        arrUsedHashMap.put(curArrName, num+1);
//                        countArrforVPK++;
//                    }
                    int num = arrUsedHashMap.get(curArrName);
                    arrUsedHashMap.put(curArrName, num+1);
                    countArrforVPK++;
                }
            }else{
                if (tempArrHashMSBbi.compareTo(curTupleAvgArr)==-1){
                    String tempStr = tempArrHashMSBbi.toString();
                    tempConArrToVPK.append(tempStr);
                    String curArrName = arrtributes[tempArrIndex];
//                    // 查看属性是否已经被删除，删除则该属性不能构建
//                    for (int i=0;i<delArrs.length;i++){
//                        if (delArrs[i]==curArrName)
//                            delFlag=1;
//                    }
//                    if (delFlag==0){
//                        int num = arrUsedHashMap.get(curArrName);
//                        arrUsedHashMap.put(curArrName, num+1);
//                        countArrforVPK++;
//                    }
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

    public static String[] getFinalArrtributes(String[] arrtributes, String[] delArrs){
        int delArrsLen = delArrs.length;
        int arrsLen = arrtributes.length;
        String[] finalArrtributes = {};
        if(delArrsLen==0){
            return arrtributes;
        }
        LinkedList<String> origin = new LinkedList<>();
        for (String str : arrtributes){
            if (!origin.contains(str)) {
                origin.add(str);
            }
        }
        for (int i=0;i<delArrsLen;i++){
            String delArr = delArrs[i];
            if (origin.contains(delArr)) {
                origin.remove(delArr);
            }
        }
        return origin.toArray(finalArrtributes);
    }

    /**
     * 水塘抽样算法，进行元组删除实验
     * @param tupleLimit
     * @param k
     * @return
     */
    public static int[] reservoirSample(int tupleLimit, int k) {
        int[] input = new int[tupleLimit];
        for (int i=0; i<tupleLimit; i++){
            input[i] = i+1;
        }
        int[] ret = new int[k];
        for (int  i = 0; i < tupleLimit; i++) {
            if (i < k) {
                ret[i] = input[i];
            } else {
                int rand = new Random().nextInt(i);
                if (rand < k) {
                    ret[rand] = input[i];
                }
            }
        }

        return ret;
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

}
