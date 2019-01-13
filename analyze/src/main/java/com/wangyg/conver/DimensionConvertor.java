package com.wangyg.conver;

import com.wangyg.bean.BaseDimension;
import com.wangyg.bean.Contactdimension;
import com.wangyg.bean.DateDimension;
import com.wangyg.util.JDBCUtil;
import com.wangyg.util.LRUCache;

import java.sql.*;

public class DimensionConvertor {
    //初始化LRUCache
    private LRUCache lruCache = new LRUCache(1000);

    //插入数据，返回ID
    public int getId(BaseDimension baseDimension) throws SQLException {
        //获取缓存的key
        String lruCacheKey = getLruCacheKey(baseDimension);

        //首先在lruCache中进行查找
        if(lruCache.containsKey(lruCacheKey)){
            return lruCache.get(lruCacheKey); // 如果已经存在于lrucache中，直接返回，说明已经插入成功
        }

        //查询数据
        //如果没有查询到数据，就插入数据
        //再次查询
        int id = exeSql (baseDimension);

        if(id==0){
            //如果id为0表示没有插入，查询成功--手动抛出异常
            throw  new RuntimeException("未找到指定维度!!!");
        }

        //5.将查询结果放入缓存
        lruCache.put(lruCacheKey, id);

        return id;
    }

    /**
     *  执行sql语句，再次执行查询语句，返回id
     * @param baseDimension
     * @return
     */
    private int exeSql(BaseDimension baseDimension) throws SQLException {
       int id =0;
       //获取mysql连接
        Connection connection = JDBCUtil.getInstance();
        //获取sql语句
        String[] sqls = getSqls(baseDimension);
        //进行第一次查询
        PreparedStatement preparedStatement = connection.prepareStatement(sqls[0]);

        //sql语句赋值
        setArguments(preparedStatement, baseDimension);

        //执行查询，获取结果
        ResultSet resultSet = preparedStatement.executeQuery();
        if(resultSet.next()){//判断是否有结果
            id= resultSet.getInt(1);
        }else{ // 不存在，先进行插入操作，在执行查询操作

            //执行插入数据操作
            preparedStatement = connection.prepareStatement(sqls[1]);
            setArguments(preparedStatement, baseDimension);
            preparedStatement.executeUpdate();// 执行update操作

            //进行第二次查询数据
            preparedStatement = connection.prepareStatement(sqls[0]);
            setArguments(preparedStatement, baseDimension);
            resultSet = preparedStatement.executeQuery();
            if(resultSet.next()){
                id = resultSet.getInt(1);
            }

        }

        //资源进行关闭
        JDBCUtil.clost(resultSet, preparedStatement, null);
        return id;
    }

    private void setArguments(PreparedStatement preparedStatement, BaseDimension baseDimension) throws SQLException {

        //联系人维度
        if (baseDimension instanceof Contactdimension) {
            Contactdimension contactDimension = (Contactdimension) baseDimension;

            preparedStatement.setString(1, contactDimension.getPhone());
            preparedStatement.setString(2, contactDimension.getName());

        } else {
            //时间维度
            DateDimension dateDimension = (DateDimension) baseDimension;

            preparedStatement.setInt(1, Integer.parseInt(dateDimension.getYear()));
            preparedStatement.setInt(2, Integer.parseInt(dateDimension.getMonth()));
            preparedStatement.setInt(3, Integer.parseInt(dateDimension.getDate()));
        }
    }

    /**
     *  获取sql语句
     * @param baseDimension
     * @return
     */
    private String[] getSqls(BaseDimension baseDimension) {

        String[] sqls = new String[2];

        //联系人维度
        if (baseDimension instanceof Contactdimension) {
            sqls[0] = "SELECT `id` FROM `tb_contacts` WHERE `telephone`=? AND `name`=?";
            sqls[1] = "INSERT INTO `tb_contacts` VALUES(null,?,?)";
        } else {
            sqls[0] = "SELECT `id` FROM `tb_dimension_date` WHERE `year`=? AND `month`=? AND `day`=?";
            sqls[1] = "INSERT INTO `tb_dimension_date` VALUES(null,?,?,?)";
        }

        return sqls;
    }

    //首先获取lrucache--baseDimension可能有两种情况，封装个对象，进行判断
    private String getLruCacheKey(BaseDimension baseDimension) {
        if (baseDimension instanceof Contactdimension) {
            //如果是联系人维度
            Contactdimension contactDimension = (Contactdimension) baseDimension;
            return contactDimension.getPhone();
        }

        //如果是时间维度
        DateDimension dateDimension = (DateDimension) baseDimension;
        return dateDimension.getYear() + "_" + dateDimension.getMonth() + "_" + dateDimension.getDate();

    }

}
