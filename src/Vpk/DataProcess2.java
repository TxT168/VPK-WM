package Vpk;

import EmbedAndExtract.EmbedAndExtract;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.*;

public class DataProcess2 {

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

    public static HashMap<String, Integer> genVPK4(double[] prob, String tablename, int tupleLimitStart, int tupleLimit, String sk, int lsbSize, String[] params, int[][] imgWM, int[][] EmbedWMPos, int msbSize){
        Connection con = JDBCUtil.getConnection();
        PreparedStatement pstmt = null;
        PreparedStatement pstmtEmbed = null;
        ResultSet rs = null;

        // sortArrsAndProbs(params, prob);
        HashMap<String, Integer> vpk = new HashMap<>();
        int valPosLimit = 1;   //二进制Hash字符串可使用的比特位
        int len = params.length;        // 没有删除属性前，属性的个数
        int width = imgWM[0].length;    // 水印图片的宽度
        int height = imgWM.length;      // 水印图片的高度
        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width; j++) {
                EmbedWMPos[i][j] = 0;
            }
        }
        char[][] probBin = new char[len][];
//        for (int i = 0; i < len; i++){
//            probBin[i] = DataProcess5.probToBinByte2(prob[i]);        //将每个属性的使用概率转换为字符数组
//            // System.out.println("当前属性" + params[i] + "的比例二进制串" + String.copyValueOf(probBin[i]));
//        }
        probBin = DataProcess5.allArrfinalProbToBinByte2(prob, params, tablename, tupleLimit, lsbSize, sk);
        //System.out.println("当前属性的比例二进制串" + String.copyValueOf(probBin[0]));

        try{
            // 构建sql查询语句
            String arrStr = "id";
            for (int i = 0; i < len; i++) {
                arrStr = arrStr + "," + params[i];
            }
            String sql = "select " + arrStr + " from " + tablename + " where "+ tablename + ".id" +" between " + tupleLimitStart + " and " + tupleLimit;
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
                int valPos = 1;        //默认用最右比特位开始判断
                int curValPosLimit = valPosLimit;   //二进制Hash字符串可使用的比特位
                Integer tempVpkRepeatNum = 0;  //当前元组构成的临时VPK在VPK集中的重复个数

                //取当前元组的各个属性值的MSB部分，并与sk进行hash，得到各属性的hash值
                BigInteger curTupleArrMSBHashMax = new BigInteger("0");
                List<String> arrsUsedHashToVpk = new ArrayList<>();
                String[] tempHash = new String[len];
                char[][] charHash = new char[len][];
                char[][] curTupleArrHashBinChar = new char[len][];
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
                        // 将MSB二进制串转换为十进制，规避一下属性值修改攻击的影响
                        String tempArrBinStrMSBHash = testencryptMD5(sk.concat(arrBinStrMSB));
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

                // 根据Amax决定该元组的选取属性MSB二进制截取的长度
                // BigInteger scale = new BigInteger("112");

                // Amax为偶数，则从小到大组装；Amax为奇数，则从大到小组装
                BigInteger biDirection = curTupleArrMSBHashMax.mod(new BigInteger("2"));
                int direction = biDirection.intValue();     // direction=0 偶数， direction=1 奇数

                while (curValPosLimit>0){
                    List<String> tempUsedArrToVPK = new ArrayList<>();     // 记录当前元组暂时选用那些属性来构建VPK（属性名）
                    for (int k = 0; k < len; k++) {
                        int prolen = probBin[k].length;    //当前元组某个属性的概率字节数组的长度
                        String arrCur = params[k];

                        // 当前属性满足要求属性就跳过
                        Integer tempArrNumUsed = hashMap.get(arrCur);
                        if (tempArrNumUsed >= tupleLimit * (prob[k]+0.02)){
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
                                if (i == prolen-1 && prob[k]!=0.5){
                                    String tempHashStr = tempHash[k];
                                    String tempScaleLen = String.valueOf(tempHashStr.length());
                                    BigInteger temp = new BigInteger(tempHashStr,2);
                                    int tempPos = curTupleArrMSBHashMax.add(temp).mod(new BigInteger(tempScaleLen)).intValue()+1;
                                    // int tempPos = temp.mod(new BigInteger(tempScaleLen)).intValue()+1;
                                    arrsUsedHashToVpk.add(tempHash[k].substring(0, tempPos));
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
                                arrsUsedHashToVpk.add(tempHash[k].substring(0, tempPos));
                                // conStr = conStr.concat(tempHashStr.substring(0, tempPos));
                                tempUsedArrToVPK.add(arrCur);
                                break;
                            }else if (curCharHash=='1' && probBin[k][i]=='0') {
                                break;
                            }
                        }

                        //调整当前属性的选取概率
                        //prob[k] = adaptiveProb(prob[k], originProb[k], hashMap.get(arrCur), curTupleCount);

                    }

                    if (tempUsedArrToVPK.size()==0){
                        valPos++;           //如果在当前valPos值下该元组conStr为空，则继续使用写一个位置
                        curValPosLimit--;      //可用位置减少
                    }
                    else{
                        // 根据direction组装vpk，偶数，从小到大,顺时针；奇数，从大到小，逆时针
                        conStr = arrsUsedConcat(arrsUsedHashToVpk, curTupleArrMSBHashMax, direction);
                        conStr = testencryptMD5(sk.concat(conStr));
                        //判断该元组的vpk是否重复，重复则conStr为空
                        String str = new BigInteger(conStr,2).toString(10);
                        tempVpkRepeatNum = vpk.get(str);
                        if (tempVpkRepeatNum == null){
                            vpk.put(str, 1);
                            for(String arrName : tempUsedArrToVPK){
                                int num = hashMap.get(arrName);
                                hashMap.put(arrName, num+1);
                            }
                            // 利用当前vpk进行嵌入水印
                            // EmbedAndExtract.EmbedWatermark(imgWM, width, height, str, curTupleArrHashBinChar, len, lsbSize, tablename, rs, params, con, pstmtEmbed, EmbedWMPos);
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
                                // 利用当前vpk进行嵌入水印
                                // EmbedAndExtract.EmbedWatermark(imgWM, width, height, str, curTupleArrHashBinChar, len, lsbSize, tablename, rs, params, con, pstmtEmbed, EmbedWMPos);
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

            for (String arrName : params){
                int arrCount = hashMap.get(arrName);
                System.out.println("属性" + arrName + "使用了："+ arrCount +"/"+ (tupleLimit-tupleLimitStart+1) +" "+ (float)arrCount/(tupleLimit-tupleLimitStart+1));
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

    public static void delCompare(HashMap<String, Integer> origin_vpk, HashMap<String, Integer> delArr_vpk){
        // List<String> intersection = delArr_vpk.stream().filter(item -> origin_vpk.contains(item)).collect(toList());        // 找两个list的交集
        // int keep_number = intersection.size();     // 删除属性后，保持不变的VPK数量
        // int change_number = origin_vpk.size()-keep_number;   //删除属性后，被破坏的VPK数量

        int keep_number = 0;
        int change_number = 0;
        for (String oriArrVpk : origin_vpk.keySet()){
            if (delArr_vpk.get(oriArrVpk) != null){
                keep_number++;
            }else{
                change_number++;
            }
        }
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



    public static void evaluateIndicator_One(HashMap<String, Integer> vpk, int tupleLimit, int tuplestart){
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
        p = (duplicateGroupFraction+(float)exclusiveNum) * 100 / (tuplestart+tupleLimit-1);

        System.out.println("VPK排他值有" + exclusiveNum);
        System.out.println("Group数：" + count);
        System.out.println("Group中最大size为：" + max);
        System.out.println("指标p：" + p);
    }

    public static float evaluateIndicator_Two(int[][] originImgWM, int[][] extractImgWM, int height, int width){
        float cf = 0f;
        int sum = 0;
        int count = 0;
        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width; j++) {
                int tempExtractPixel = extractImgWM[i][j];
                // 当前提取的像素为 1 白色
                if (tempExtractPixel==-1){
                    int tempXor = originImgWM[i][j] ^ 0;
                    if (tempXor == 0){
                        count++;
                    }
                    sum = sum + tempXor;
                }
                // 当前提取的像素为 0 黑色
                else{
                    int tempXor = originImgWM[i][j] ^ 1;
                    if (tempXor == 0){
                        count++;
                    }
                    sum = sum + tempXor;
                }
            }
        }
        System.out.println("与原来的图像像素不同的数量：" + count);
        cf = sum / (float)(height * width) * 100;
        return cf;
    }

    public static int[][] embedWMByVPK(int[][] originImgWM, int[][] EmbedWMPos, int height, int width){
        int [][] WmAndNoWmPos = new int[height][width];
        for (int i = 0; i < height; i++) {
            for (int j = 0; j < width; j++) {
                if (EmbedWMPos[i][j]==0){
                    WmAndNoWmPos[i][j] = -4194304;
                }else{
                    if (originImgWM[i][j]==1)
                        WmAndNoWmPos[i][j] = -1;
                    else {
                        WmAndNoWmPos[i][j] = -16777216;
                    }
                }
            }
        }
        return WmAndNoWmPos;
    }

    // 给属性和比例数组进行重新排序
    public static void sortArrsAndProbs(String[] arrtributes, double[] probs){
        SortedMap<String, Double> arrsAndProbs = new TreeMap<>();
        int len = arrtributes.length;
        for (int i=0; i<len; i++){
            arrsAndProbs.put(arrtributes[i], probs[i]);
        }
        int j = 0;
        for (String arrName : arrsAndProbs.keySet()){
            arrtributes[j] = arrName;
            probs[j] = arrsAndProbs.get(arrName);
            j++;
        }
    }

    // 根据该元组的最大值，在已经被选择的排好序的元组中，选择开始合并的属性值
    public static String arrsUsedConcat (List<String> arrsUsedHashToVpk, BigInteger curTupleArrMSBHashMax, int direction){
        StringBuffer conStrBf = new StringBuffer();
        int len = arrsUsedHashToVpk.size();
        BigInteger biLen = BigInteger.valueOf(len);
        // Amax为偶数
        if (direction==0){
            Collections.sort(arrsUsedHashToVpk);
        }else{
            Collections.reverse(arrsUsedHashToVpk);
        }
        int startPos = curTupleArrMSBHashMax.mod(biLen).intValue();
        int tempPos = startPos;
        for(int i=0; i<len; i++){
            conStrBf.append(arrsUsedHashToVpk.get(tempPos));
            tempPos++;
            if (tempPos>=len){
                tempPos = tempPos-len;
            }
        }
        return conStrBf.toString();
    }
}
