package Vpk;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class DataProcess5 {
    /**
     * 将一个属性的所有值转换为MSB二进制字符串，并存储在一个hashmap中
     * @param arrName
     * @param tablename
     * @param lsbSize
     * @param sk
     * @return
     * @throws SQLException
     */
    public static HashMap<Integer, char[]> arrSetToMSBHashMap(String arrName, String tablename, int lsbSize, String sk, int tupleSum) {
        int curTupleCount = 0;  //记录当前元组的总数
        HashMap<Integer, char[]> curArrHashMap = new HashMap<>();

        //创建连接
        Connection con = JDBCUtil.getConnection();
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "select " + arrName + " from " + tablename + " LIMIT " + tupleSum;
        try{
            //绑定sql语句
            pstmt = con.prepareStatement(sql);
            //执行语句，用结果集接收
            rs=pstmt.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            while(rs.next()) {
                curTupleCount++;            //统计使用的元组数
                String arrBinStrMSB = null;        // 截取每个属性值二进制串的最大有效位 msb

                String curArrType = metaData.getColumnTypeName(1);
                String tempStr = null;
                if (curArrType == "INT") {
                    tempStr = Integer.toBinaryString(rs.getInt(1));
                } else if (curArrType == "FLOAT") {
                    int temp2 = Float.floatToIntBits(rs.getFloat(1));
                    tempStr = Integer.toBinaryString(temp2);
                } else if (curArrType == "DOUBLE") {
                    long temp3 = Double.doubleToLongBits(rs.getDouble(1));
                    tempStr = Long.toBinaryString(temp3);
                }

                // 截取每个属性值二进制串的最大有效位 msb
                int tempStrLen = tempStr.length();

                if (tempStrLen > lsbSize) {
                    arrBinStrMSB = tempStr.substring(0, tempStrLen - lsbSize);
                    char[] charHashStrMSB = DataProcess2.testencryptMD5(sk.concat(arrBinStrMSB)).toCharArray();
                    curArrHashMap.put(curTupleCount, charHashStrMSB);
                } else {
                    char[] charHashStrMSB = {'N'};
                    curArrHashMap.put(curTupleCount, charHashStrMSB);    // MSB不够比特位，所以字符串赋值为 N
                }
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally{
            JDBCUtil.DBClose(con, pstmt, rs);
        }
        return curArrHashMap;
    }

    public static char[] arrProbBinStr(double arrProb, HashMap<Integer, char[]> curArrHashMap, int tupleSum){
        double prob = arrProb;
        int needArrNum = (int)(tupleSum * arrProb);
        char[] arrProbBin = probToBinByte2(prob);
        char[] final_probBinChars;
        int probLen = arrProbBin.length;                    // 属性的概率字节数组的长度

        HashMap<Integer, Boolean> recordList = new HashMap<>();       // 记录当前符合比例的属性值的index
        List<Integer> abandonList = new ArrayList<>();
        // 判断当前比例下满足要求的元组数是否超过要求的个数，超过进入下一步
        // 没有超过的话，调高比例，直到满足的个数超过要求的个数
        boolean meetFlag = false;
        int probBinStrFinalPos = 0;
        int countMeetArrValue = 0;                          // 统计满足条件的属性值的个数
        int laseKeySetSize = 0;

        while (!meetFlag){
            for (int i = 1; i <= probLen; i++) {
                for (int key : curArrHashMap.keySet()) {
                    if (recordList.get(key) != null) {
                        continue;                  // 已经被收录，就看后面的bit了
                    }

                    char[] charHashStrMSB = curArrHashMap.get(key);
                    int charHashStrLen = charHashStrMSB.length;
                    if (charHashStrLen==1){
                        abandonList.add(key);
                        continue;
                    }
//                    if (i>charHashStrLen){
//                        abandonList.add(key);
//                        continue;
//                    }
                    char curCharHash = charHashStrMSB[charHashStrLen-i];

                    if (curCharHash=='0'){
                        if (i==probLen){                     // && arrProbBin[i-1]=='1'
                            abandonList.add(key);
                        }
                        //recordList.add(key);
                    }else if (curCharHash=='1' && arrProbBin[i-1]=='1') {
                        recordList.put(key, true);
                    }else if (curCharHash=='1' && arrProbBin[i-1]=='0') {
                        abandonList.add(key);
                    }
                }

                // 遍历所有的属性值，查看当前满足的比例是否在误差范围内，不管是多了还是少了
                Set<Integer> tempKeySet = curArrHashMap.keySet();
                laseKeySetSize = curArrHashMap.size();
                tempKeySet.removeAll(abandonList);
                //tempKeySet.retainAll(recordList);
                countMeetArrValue = curArrHashMap.size();
                //System.out.println("当前所选的属性数有：" + countMeetArrValue);
                if (Math.abs(countMeetArrValue - needArrNum) <= (float)tupleSum * 0.01){
                    // 删减hashmap中不符合当前比例的数据
                    //Set<Integer> tempKeySet = curArrHashMap.keySet();
                    //tempKeySet.retainAll(recordList);
                    probBinStrFinalPos = i-1;
                    meetFlag = true;
                    System.out.println(countMeetArrValue + "/" + tupleSum + " " + countMeetArrValue/(float)tupleSum);
                    System.out.println("满足精度！");
                    break;
                }else{
                    // 误差范围外，如果已经超过要求的个数，继续加入下一个比例往下走；
                    if (countMeetArrValue > needArrNum){
                        // 删减hashmap中不符合当前比例的数据
                        //Set<Integer> tempKeySet = curArrHashMap.keySet();
                        //tempKeySet.retainAll(recordList);

                        // 重新清空recordList，在下一个比例中继续做记录
                        //recordList.clear();
                        abandonList.clear();
                        if (i==probLen){
                            probBinStrFinalPos = i-1;
                            System.out.println("到达最大精度，仍然比需要的多！");
                            System.out.println(countMeetArrValue + "/" + tupleSum + " " + countMeetArrValue/(float)tupleSum);
                            meetFlag = true;
                            break;
                        }
                    }else {
                        // 看上次精度下的属性个数与需要的个数的差值 和 这次精度下的属性个数与需要的个数的差值
                        // 谁小，取那个精度
                        // 当前精度，差得更多
                        //probBinStrFinalPos = i-2;
                        if ((needArrNum - countMeetArrValue) * 5 < laseKeySetSize - needArrNum){
                            System.out.println("选择不足");
                            probBinStrFinalPos = i-1;
                            System.out.println(countMeetArrValue + "/" + tupleSum + " " + countMeetArrValue/(float)tupleSum);
                        }else{
                            probBinStrFinalPos = i-2;
                            System.out.println(laseKeySetSize + "/" + tupleSum + " " + laseKeySetSize/(float)tupleSum);
                        }
                        meetFlag = true;

                        break;
                    }
                }
            }

        }

        final_probBinChars = java.util.Arrays.copyOf(arrProbBin, probBinStrFinalPos+1);
        System.out.println("当前属性比例二进制最后的拟合字符串：" + String.copyValueOf(final_probBinChars));
        return final_probBinChars;
    }

    /**
     * 比例转换为二进制串
     * 方法2：自定义的方法
     * @param prob
     * @return
     */
    public static char[] probToBinByte2(double prob) {
        StringBuffer stringBuffer = new StringBuffer();
        String probBinString;
        int probBinStringLen = 8;        // 最大的拟合二进制串的长度为128位
        double sum = 0;
        for (int i = 1; i <= probBinStringLen; i++) {
            double temp = sum + Math.pow(0.5, i);
            if (temp > prob){
                stringBuffer.append("0");
            }else if(prob > temp){
                stringBuffer.append("1");
                sum = temp;
            }else {
                stringBuffer.append("1");
                break;
            }
        }
        probBinString = stringBuffer.toString();
        char[] binStringChars = probBinString.toCharArray();
        //System.out.println("当前属性的二进制拟合串：" + String.copyValueOf(binStringChars));
        return binStringChars;
    }

    /**
     * 将原来最初的二进制拟合串根据实际在数据集中的个数在精度上进行一定的简化
     * 最后只保留满足个数超过约定个数的精度
     * @param arrBinStrChars
     * @return
     */
    public static char[] finalProbToBinByte2(char[] arrBinStrChars, double prob, String arrName, String tablename, int tupleLimit, int lsbSize, String sk){
        int arrBinStrCharsLen = arrBinStrChars.length;
        char[] finalArrBinStringChars = new char[arrBinStrCharsLen];
        StringBuffer sb = new StringBuffer();
        int arrMeetNum = 0;
        int preArrMeetNum = 0;
        for (int i = 0; i < arrBinStrCharsLen; i++){
            preArrMeetNum = arrMeetNum;
            sb.append(arrBinStrChars[i]);
            char[] tempArrBinStrChars = sb.toString().toCharArray();
            arrMeetNum = curProbBinCharMeetNum(tempArrBinStrChars, prob, arrName, tablename, tupleLimit, lsbSize, sk);
            // 如果当前满足
            if (arrMeetNum < (int)(tupleLimit * prob)){
                sb.deleteCharAt(i);
                // System.out.println("属性可能的个数：" + preArrMeetNum);
                break;
            }
//            else {
//                if (i==arrBinStrCharsLen-1){
//                    // System.out.println("属性可能的个数：" + preArrMeetNum);
//                }
//            }
        }
        finalArrBinStringChars = sb.toString().toCharArray();
        return finalArrBinStringChars;
    }

    public static char[][] allArrfinalProbToBinByte2(double[] prob, String[] arrtributes, String tablename, int tupleLimit, int lsbSize, String sk){
        int probsLen = prob.length;
        char[][] allArrFinalProbBin = new char[probsLen][];
        for (int i = 0; i < probsLen; i++) {
            char[] arrBinStrChars = probToBinByte2(prob[i]);
            char[] finalArrBinStringChars = finalProbToBinByte2(arrBinStrChars, prob[i], arrtributes[i], tablename, tupleLimit, lsbSize, sk);
            System.out.println("当前属性"+ arrtributes[i] +"的最终的二进制拟合串：" + String.copyValueOf(finalArrBinStringChars));
            allArrFinalProbBin[i] = finalArrBinStringChars;
        }
        return allArrFinalProbBin;
    }

    /**
     * 获取某个属性在当前精度下满足条件的属性个数
     * @param curProbBinChar
     * @param prob
     * @param arrName
     * @param tablename
     * @param tupleLimit
     * @param lsbSize
     * @param sk
     * @return
     */
    public static int curProbBinCharMeetNum(char[] curProbBinChar, double prob, String arrName, String tablename, int tupleLimit, int lsbSize, String sk){
        int arrMeetNum = 0;
        int valPos = 1;        //默认用最右比特位开始判断

        Connection con = JDBCUtil.getConnection();
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try{
            // 构建sql查询语句
            String sql = "select " + arrName + " from " + tablename + " LIMIT " + tupleLimit;
            //预编译sql语句，防止sql注入
            pstmt = con.prepareStatement(sql);
            //执行语句，用结果集接收
            rs = pstmt.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();

            int curTupleCount = 0;        //目前对多少个元组进行了操作

            while(rs.next()){
                curTupleCount++;

                //取当前元组的该属性值的MSB部分，并与sk进行hash，得到各属性的hash值
                String tempHash;
                char[] charHash;

                String arrBinStrMSB = null;        // 截取每个属性值二进制串的最大有效位 msb

                String curArrType = metaData.getColumnTypeName(1);
                String tempStr = null;
                if (curArrType == "INT") {
                    tempStr = Integer.toBinaryString(rs.getInt(1));
                }else if (curArrType == "FLOAT"){
                    int temp2 = Float.floatToIntBits(rs.getFloat(1));
                    tempStr = Integer.toBinaryString(temp2);
                }else if (curArrType == "DOUBLE"){
                    long temp3 = Double.doubleToLongBits(rs.getDouble(1));
                    tempStr = Long.toBinaryString(temp3);
                }

                // 截取每个属性值二进制串的最大有效位 msb
                int tempStrLen = tempStr.length();

                if (tempStrLen>lsbSize){
                    arrBinStrMSB = tempStr.substring(0, tempStrLen-lsbSize);
                    tempHash = DataProcess2.testencryptMD5(sk.concat(arrBinStrMSB));
                    charHash = tempHash.toCharArray();
                }else {
                    tempHash = "N";
                    char[] tempcharHash = {'N'};
                    charHash = tempcharHash;
                }

                if (tempHash=="N"){
                    continue;
                }

                int prolen = curProbBinChar.length;    //当前元组某个属性的概率字节数组的长度
                int charHashlen = tempHash.length();

                for (int i = 0; i < prolen; i++) {
                    char curCharHash = charHash[charHashlen-i-valPos];
                    if (curCharHash=='0'){
                        if (i == prolen-1 && prob!=0.5){
                            arrMeetNum++;
                            break;
                        }
                        continue;
                    }else if (curCharHash=='1' && curProbBinChar[i]=='1') {
                        arrMeetNum++;
                        break;
                    }else if (curCharHash=='1' && curProbBinChar[i]=='0') {
                        break;
                    }
                }
            }
        } catch(SQLException e){
            e.printStackTrace();
        } finally{
            JDBCUtil.DBClose(con,pstmt,null);
        }
        return arrMeetNum;
    }

//    public static HashMap<Integer, String> getFinalVPK(double[] probs, String[] arrNames, int tupleSum, String tablename, String sk){
//        int len = probs.length;
//        HashMap<Integer, String> vpk = new HashMap<>();
//        for (int i = 0; i < len; i++) {
//            String curArrName = arrNames[i];
//            HashMap<Integer, char[]> curArrHashMap = arrSetToMSBHashMap(curArrName, tablename, 3, sk, tupleSum);
//            double curArrProb = probs[i];
//            HashMap<Integer, char[]> arrHashMap = selectHashStringToProb(curArrProb, curArrHashMap, tupleSum);
//            for (Integer key : arrHashMap.keySet()){
//                String tempVpkArrHashBinStr = vpk.get(key);
//                String arrHashBinStr = String.copyValueOf(arrHashMap.get(key));
//                if (tempVpkArrHashBinStr != null) {
//                    tempVpkArrHashBinStr = tempVpkArrHashBinStr.concat(arrHashBinStr);
//                    vpk.put(key, tempVpkArrHashBinStr);
//                }else{
//                    vpk.put(key, arrHashBinStr);
//                }
//            }
//            for (Integer vpkKey : vpk.keySet()){
//                String finalOneVPK = DataProcess2.testencryptMD5(sk.concat(vpk.get(vpkKey)));
//                vpk.put(vpkKey, finalOneVPK);
//            }
//        }
//        return vpk;
//    }

    public static char[][] allProbHashBinChars(double[] probs, String[] arrNames, int tupleSum, String tablename, String sk, int lsbSize){
        int len = probs.length;
        char[][] allArrProbHashBin = new char[len][];
        for (int i = 0; i < len; i++) {
            HashMap<Integer, char[]> curArrHashMap = arrSetToMSBHashMap(arrNames[i], tablename, lsbSize, sk, tupleSum);
            double curArrProb = probs[i];
            allArrProbHashBin[i] = arrProbBinStr(curArrProb, curArrHashMap, tupleSum);
        }
        return allArrProbHashBin;
    }

    public static HashMap<String, Integer> countVpkRepeat(HashMap<Integer, String> final_VPK){
        HashMap<String, Integer> hashMap = new HashMap<>();
        for (Integer key : final_VPK.keySet()){
            String oneVpk = final_VPK.get(key);
            if (hashMap.get(oneVpk) != null) {
                Integer value = hashMap.get(oneVpk);
                hashMap.put(oneVpk, value+1);
            } else {
                hashMap.put(oneVpk, 1);
            }
        }
        return hashMap;
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
            }else{
                count++;
                duplicateGroupFraction = duplicateGroupFraction + 1.0f / temp;
            }
        }
        p = (duplicateGroupFraction+(float)exclusiveNum) * 100 / tupleLimit;

        System.out.println("VPK排他值：" + exclusiveNum);
        System.out.println("Group数：" + count);
        System.out.println("Group中最大size为：" + max);
        System.out.println("指标p：" + p);
    }

    public static void main(String[] args) throws SQLException {
        String tablename = "forest";
        String sk = "100101000111";
        int lsbSize = 3;
        int tupleSum = 30000;
        double[] probs = {0.3, 0.3, 0.3, 0.3, 0.3, 0.3, 0.3};
        String[] arrtributes = {"Elevation", "Aspect", "Slope", "Horizontal_Hydrology", "Vertical_Hydrology",           // "Slope"  "Hillshade_9am", "Hillshade_Noon"
                "Horizontal_Roadways", "Hillshade_9am", "Hillshade_Noon", "Hillshade_3pm", "Horizontal_Fire_Points"};
        // 测试
        double[] probs2 = {0.3};
        String[] arrtributes2 = {"Elevation"};
        char[][] allArrProbHashBin = allProbHashBinChars(probs2, arrtributes2, tupleSum, tablename, sk, lsbSize);
        for(char[] chars : allArrProbHashBin){
            System.out.println(String.copyValueOf(chars));
        }

        //HashMap<String, Integer> countVpkRe = countVpkRepeat(final_VPK);
        //evaluateIndicator_One(countVpkRe, tupleSum);


    }

}
