package EmbedAndExtract;

import Vpk.DBDao;
import Vpk.DataProcess2;

import GetData.GetImage2Array;
import Vpk.DataProcess5;
import Vpk.JDBCUtil;

import java.awt.image.BufferedImage;
import java.io.ObjectStreamException;
import java.math.BigInteger;
import java.sql.*;
import java.util.*;

public class EmbedAndExtract {
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

    public static void ExtractPixelWatermark(int[][] ExtractWM, int width, int height, String vpk, HashMap<String, char[]> curTupleArrBinChar, int originArrNum, int lsbSize, String[] arrtributes, String[] delArrs){

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

    public static int[][] ExtractWM(double[] probs, int width, int height, int lsbSize, String tablename,
                                    String[] arrtributes, int tuplestart, int tupleLimit, String sk, String[] delArrs, int remainTupleNum){

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
        int originArrsNum = arrtributes.length;
        double[] finalProbs = new double[originArrsNum-delArrs.length];
        String[] finalArrtributes = getFinalArrtributesAndProbs(arrtributes, delArrs, finalProbs, probs);
        // DataProcess2.sortArrsAndProbs(finalArrtributes, finalProbs);

        int len = finalArrtributes.length;
        int valPosLimit = 1;   //二进制Hash字符串可使用的比特位
        char[][] probBin = DataProcess5.allArrfinalProbToBinByte2(finalProbs, finalArrtributes, tablename, tupleLimit, lsbSize, sk);

        int[] delRemainTupleIndex = reservoirSample(tuplestart, tupleLimit, remainTupleNum);

        int countNoTuple = 0;        //统计没有使用的元组
        HashMap<String, Integer> hashMap = new HashMap<String, Integer>();      //统计每个属性被使用的值的个数
        for(String string : finalArrtributes){
            hashMap.put(string, 0);
        }

        try{
            for (int tupleIndex : delRemainTupleIndex){
                // 构建SQL查询语句
                String arrStr = "id";
                for (int i = 0; i < len; i++) {
                    arrStr = arrStr + "," + finalArrtributes[i];
                }
                // String sql = "select " + arrStr + " from " + tablename + " where "+ tablename + ".id" +" between 0 and " + tupleLimit;
                String sql = "select " + arrStr + " from " + tablename + " where "+ tablename + ".id=" + tupleIndex;
                //预编译sql语句，防止sql注入
                pstmt = con.prepareStatement(sql);
                //执行语句，用结果集接收
                rs = pstmt.executeQuery();
                ResultSetMetaData metaData = rs.getMetaData();

                while(rs.next()){

                    String conStr = "";
                    int valPos = 1;        //默认用最右比特位开始判断
                    int curValPosLimit = valPosLimit;   //二进制Hash字符串可使用的比特位
                    Integer tempVpkRepeatNum = 0;  //当前元组构成的临时VPK在VPK集中的重复个数

                    //取当前元组的各个属性值的MSB部分，并与sk进行hash，得到各属性的hash值
                    BigInteger curTupleArrMSBHashMax = new BigInteger("0");
                    List<String> arrsUsedHashToVpk = new ArrayList<>();
                    String[] tempHash = new String[len];
                    char[][] charHash = new char[len][];
                    //char[][] curTupleArrHashBinChar = new char[len][];
                    HashMap<String, char[]> curTupleArrHashBinChar = new HashMap<>();
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
                        curTupleArrHashBinChar.put(finalArrtributes[k], tempStr.toCharArray());

                        if (tempStrLen>lsbSize){
                            arrBinStrMSB = tempStr.substring(0, tempStrLen-lsbSize);
                            // 将MSB二进制串转换为十进制，规避一下属性值修改攻击的影响
                            String tempArrBinStrMSBHash = DataProcess2.testencryptMD5(sk.concat(arrBinStrMSB));
                            BigInteger decimalArrMSB = new BigInteger(tempArrBinStrMSBHash,2);
                            if (decimalArrMSB.compareTo(curTupleArrMSBHashMax)==1){
                                curTupleArrMSBHashMax = decimalArrMSB;
                            }
                            tempHash[k] = tempArrBinStrMSBHash;
                            charHash[k] = tempArrBinStrMSBHash.toCharArray();
                        }else {
                            tempHash[k] = "0";            // 为空
                            char[] tempCharHash = {'0'};
                            charHash[k] = tempCharHash;
                        }
                    }

                    // Amax为偶数，则从小到大组装；Amax为奇数，则从大到小组装
                    BigInteger biDirection = curTupleArrMSBHashMax.mod(new BigInteger("2"));
                    int direction = biDirection.intValue();     // direction=0 偶数， direction=1 奇数

                    while (curValPosLimit>0){
                        List<String> tempUsedArrToVPK = new ArrayList<>();     // 记录当前元组暂时选用那些属性来构建VPK（属性名）
                        for (int k = 0; k < len; k++) {
                            int prolen = probBin[k].length;    //当前元组某个属性的概率字节数组的长度
                            String arrCur = finalArrtributes[k];
                            // 当前属性满足要求属性就跳过
                            Integer tempArrNumUsed = hashMap.get(arrCur);
                            if (tempArrNumUsed >= tupleLimit * (finalProbs[k]+0.02)){
                                continue;
                            }

                            // 该属性的MSB不足够，跳过该属性
                            if (tempHash[k]=="0"){
                                continue;
                            }

                            int charHashlen = charHash[k].length;

                            for (int i = 0; i < prolen; i++) {
                                char curCharHash = charHash[k][charHashlen-i-valPos];
                                if (curCharHash=='0'){
                                    if (i == prolen-1 && probs[k]!=0.5){
                                        String tempHashStr = tempHash[k];
                                        String tempScaleLen = String.valueOf(tempHashStr.length());
                                        BigInteger temp = new BigInteger(tempHashStr,2);
                                        int tempPos = curTupleArrMSBHashMax.add(temp).mod(new BigInteger(tempScaleLen)).intValue()+1;
                                        // int tempPos = temp.mod(new BigInteger(tempScaleLen)).intValue()+1;
                                        arrsUsedHashToVpk.add(tempHashStr.substring(0, tempPos));
                                        // conStr = conStr.concat(tempHashStr.substring(0, tempPos));
                                        tempUsedArrToVPK.add(arrCur);
                                        break;
                                    }
                                    continue;
                                }else if (curCharHash=='1' && probBin[k][i]=='1') {
                                    String tempHashStr = tempHash[k];
                                    String tempScaleLen = String.valueOf(tempHashStr.length());
                                    BigInteger temp = new BigInteger(tempHashStr,2);
                                    int tempPos = curTupleArrMSBHashMax.add(temp).mod(new BigInteger(tempScaleLen)).intValue()+1;
                                    // int tempPos = temp.mod(new BigInteger(tempScaleLen)).intValue()+1;
                                    arrsUsedHashToVpk.add(tempHashStr.substring(0, tempPos));
                                    // conStr = conStr.concat(tempHashStr.substring(0, tempPos));
                                    tempUsedArrToVPK.add(arrCur);
                                    break;
                                }else if (curCharHash=='1' && probBin[k][i]=='0') {
                                    break;
                                }
                            }
                        }

                        if (tempUsedArrToVPK.size()==0){
                            valPos++;           //如果在当前valPos值下该元组conStr为空，则继续使用写一个位置
                            curValPosLimit--;      //可用位置减少
                        }
                        else{
                            // 根据direction组装vpk，偶数，从小到大；奇数，从大到小
                            conStr = DataProcess2.arrsUsedConcat(arrsUsedHashToVpk, curTupleArrMSBHashMax, direction);
                            conStr = DataProcess2.testencryptMD5(sk.concat(conStr));
                            String str = new BigInteger(conStr,2).toString(10);
                            //判断该元组的vpk是否重复，重复则conStr为空
                            tempVpkRepeatNum = vpk.get(str);
                            if (tempVpkRepeatNum == null){
                                vpk.put(str, 1);
                                for(String arrName : tempUsedArrToVPK){
                                    int num = hashMap.get(arrName);
                                    hashMap.put(arrName, num+1);
                                }
                                // 利用当前vpk提取水印
                                ExtractPixelWatermark(ExtractWM, width, height, str, curTupleArrHashBinChar, originArrsNum, lsbSize, arrtributes, delArrs);
                                break;
                            }
                            else {             // 有重复          && tempVpkRepeatNum >= 50
                                valPos++;           // 如果在当前valPos值下该元组conStr为空，则继续使用写一个位置
                                curValPosLimit--;      // 可用位置减少
                                if (curValPosLimit==0){
                                    //vpk.remove(conStr);
                                    for(String arrName : tempUsedArrToVPK){
                                        int num = hashMap.get(arrName);
                                        hashMap.put(arrName, num+1);
                                    }
                                    vpk.put(str, tempVpkRepeatNum+1);
                                    // 利用当前vpk提取水印
                                    ExtractPixelWatermark(ExtractWM, width, height, str, curTupleArrHashBinChar, originArrsNum, lsbSize, arrtributes, delArrs);
                                    break;
                                }
                                conStr = "";
                            }
                        }

                    }

                    if (conStr=="") {
                        countNoTuple++;
                        //System.out.println(curTupleCount);
                    }

                }
            }

            for (String arrName : finalArrtributes){
                int arrCount = hashMap.get(arrName);
                System.out.println("属性" + arrName + "使用了："+ arrCount +"/"+ tupleLimit +" "+ (float)arrCount/tupleLimit);
            }
            System.out.println("没有使用的元组: " + countNoTuple);
        } catch(SQLException e){
            e.printStackTrace();
        } finally{
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

    /*
    暂时搁置
     */
    public  static void minimizeVariation(){

    }

    /**
     * 将原来的属性数组减去删除的属性
     * @param arrtributes
     * @param delArrs
     * @return
     */
    public static String[] getFinalArrtributesAndProbs(String[] arrtributes, String[] delArrs, double[] finalProbs, double[] probs){
        int delArrsLen = delArrs.length;
        int arrsLen = arrtributes.length;
        String[] finalArrtributes = {};
        if(delArrsLen==0){
            for (int i=0;i<arrsLen;i++){
                finalProbs[i] = probs[i];
            }
            return arrtributes;
        }
        LinkedList<String> origin = new LinkedList<>();
        List<Integer> delArrsIndex = new ArrayList<>();
        for (String str : arrtributes){
            if (!origin.contains(str)) {
                origin.add(str);
            }
        }
        for (int i=0;i<delArrsLen;i++){
            String delArr = delArrs[i];
            if (origin.contains(delArr)) {
                int delArrindex = origin.indexOf(delArr);
                delArrsIndex.add(delArrindex);
            }
        }
        List<String> delArrList = new ArrayList<>();
        delArrList = Arrays.asList(delArrs);
        origin.removeAll(delArrList);
        int j = 0;
        for (int i = 0; i < arrsLen; i++) {
            if (!delArrsIndex.contains(i)){
                finalProbs[j] = probs[i];
                j++;
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
    public static int[] reservoirSample(int tuplestart, int tupleLimit, int k) {
        int[] input = new int[tupleLimit];
        for (int i=0; i<tupleLimit; i++){
            input[i] = i + tuplestart;
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

    public static void testWMpos(HashMap<String, Integer> vpkset, BigInteger height, BigInteger width){
        int[][] EmbedWMPos = new int[height.intValue()][width.intValue()];
        BigInteger bigInt = new BigInteger("2");
        for (String vpk : vpkset.keySet()){
            String hashVpk = DataProcess2.testencryptMD5(vpk.concat("1"));
            String hashVpk2 = DataProcess2.testencryptMD5(vpk.concat("2"));
            String hashVpkTen = new BigInteger(hashVpk,2).toString(10);
            String hashVpkTen2 = new BigInteger(hashVpk2,2).toString(10);
            BigInteger newHashVpkNum = new BigInteger(hashVpkTen);
            BigInteger newHashVpkNum2 = new BigInteger(hashVpkTen2);
            int hPos = newHashVpkNum.mod(height).intValue();
            int wPos = newHashVpkNum2.mod(width).intValue();
            EmbedWMPos[hPos][wPos] = EmbedWMPos[hPos][wPos] + 1;
        }
        System.out.println("haha");
    }
}
