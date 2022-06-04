package Vpk;

import java.sql.ResultSet;

//自建泛型，实现rowMappping返回T类型
public interface RowMap<T> {
    public T rowMapping(ResultSet rs);
}
