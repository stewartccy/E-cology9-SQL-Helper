package com.handler.datasources;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.ObjectUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.annotation.ColumnName;
import com.annotation.TableName;
import com.annotation.TableMapping;
import com.engine.core.exception.ECException;
import com.util.ModeServiceUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import weaver.conn.RecordSet;
import weaver.general.StringUtil;
import weaver.hrm.User;

import javax.xml.bind.Element;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

/**
 * 数据处理类
 */
public class DataSourcesHandler {

    private final Log log = LogFactory.getLog(this.getClass());

    private final RecordSet rs = new RecordSet();

    /**
     * 根据ID查询
     *
     * @param id    数据ID(不能为空)
     * @param clazz 数据类型
     * @return 数据实体
     */
    public <T> T selectById(Long id, Class<T> clazz) {
        if (ObjectUtil.isEmpty(id)) {
            throw new ECException("参数[id]不能为空");
        }
        T t = null;
        String tableName = this.getTableName(clazz);
        String executeSql = "select * from " + tableName + " where id = ?";
        rs.executeQuery(executeSql, id);
        Field[] fields = getAllFields(clazz);
        if (rs.next()) {
            Map<String, Object> map = new HashMap<>();
            for (Field field : fields) {
                String fieldName = this.getFieldName(field);
                String value = rs.getString(fieldName);
                if (ObjectUtil.isNotEmpty(value)) {
                    map.put(fieldName, value);
                }
            }
            t = JSON.parseObject(JSON.toJSONString(map), clazz);
        }
        return t;
    }

    public static void main(String[] args) {

    }

    /**
     * 根据条件查询一条数据
     *
     * @param condition 查询条件
     * @param <T>       数据类型
     * @return 查询结果
     */
    public <T> T selectOne(T condition) {
        if (null == condition) {
            return null;
        }
        List<T> list = this.selectList(condition);
        if (null != list && list.size() > 1) {
            log.error("selectOne 方法作用为查询一条数据，但查询结果中包含" + list.size() + "条数据");
            throw new ECException("selectOne 方法作用为查询一条数据，但查询结果中包含" + list.size() + "条数据");
        }
        if (null != list && list.size() == 1) {
            return list.get(0);
        } else {
            return null;
        }
    }

    /**
     * 根据条件查询
     *
     * @param condition 查询条件
     * @param <T>       数据类型
     * @return 查询结果
     */
    @SuppressWarnings({"unused", "unchecked"})
    public <T> List<T> selectList(T condition) {
        if (null == condition) {
            return null;
        }
        List<T> result = new ArrayList<>();
        Class<?> clazz = condition.getClass();
        String tableName = this.getTableName(clazz);
        StringBuilder whereKey = new StringBuilder();
        List<Object> whereValue = new ArrayList<>();
        if (ObjectUtil.isNotNull(condition)) {
            Field[] fields = this.getAllFields(clazz);
            for (Field field : fields) {
                String fieldName = this.getFieldName(field);
                Object fieldValue = this.getFieldValue(condition, field.getName());
                if (null != fieldValue) {
                    whereKey.append(" and ").append(fieldName).append(" = ?");
                    whereValue.add(fieldValue);
                }
            }
        }
        String executeSql = "select * from " + tableName + " where 1 = 1 " + whereKey.toString();
        rs.executeQuery(executeSql, whereValue);
        Field[] fields = getAllFields(clazz);
        Map<String, Object> map;
        while (rs.next()) {
            map = new LinkedHashMap<>();
            String[] columnNames = rs.getColumnName();
            for (Field field : fields) {
                String fieldName = this.getFieldName(field);
                for (String columnName : columnNames) {
                    if (columnName.toLowerCase().equals(fieldName.toLowerCase())) {
                        map.put(field.getName(), rs.getString(fieldName));
                    }
                }
            }
            result.add((T) JSON.parseObject(JSON.toJSONString(map), condition.getClass()));
        }
        return result;
    }

    /**
     * 分页查询
     *
     * @param condition 查询条件
     * @param pageStart 起始页
     * @param pageSize  每页数量
     * @param <T>       数据类型
     * @return 查询结果
     */
    @SuppressWarnings({"unused", "unchecked"})
    public <T> List<T> selectPage(T condition, Long pageStart, Long pageSize) {
        if (null == condition) {
            return null;
        }
        List<T> result = new ArrayList<>();
        Class<?> clazz = condition.getClass();
        String tableName = this.getTableName(clazz);
        StringBuilder whereKey = new StringBuilder();
        List<Object> whereValue = new ArrayList<>();
        if (ObjectUtil.isNotNull(condition)) {
            Field[] fields = this.getAllFields(clazz);
            for (Field field : fields) {
                String fieldName = this.getFieldName(field);
                Object fieldValue = this.getFieldValue(condition, fieldName);
                if (null != fieldValue) {
                    whereKey.append(" and ").append(fieldName).append(" = ?");
                    whereValue.add(fieldValue);
                }
            }
        }
        String executeSql = "select * from " + tableName + " where 1 = 1 " + whereKey.toString()
                + " limit " + pageStart + ", " + pageSize;
        rs.executeQuery(executeSql, whereValue);
        Field[] fields = getAllFields(clazz);
        Map<String, Object> map;
        while (rs.next()) {
            map = new LinkedHashMap<>();
            String[] columnNames = rs.getColumnName();
            for (Field field : fields) {
                String fieldName = this.getFieldName(field);
                for (String columnName : columnNames) {
                    if (columnName.toLowerCase().equals(fieldName.toLowerCase())) {
                        map.put(field.getName(), rs.getString(fieldName));
                    }
                }
            }
            result.add((T) JSON.parseObject(JSON.toJSONString(map), condition.getClass()));
        }
        return result;
    }

    /**
     * 自定义SQL查询一条数据
     * 该方法仅用于复杂sql
     *
     * @param clazz     数据类型
     * @param sql       自定义查询的SQL
     * @param condition 条件
     * @param <T>       数据类型
     * @return 查询结果
     */
    public <T> T customSelectOne(Class<T> clazz, String sql, Object... condition) {
        List<T> list = this.customSelectList(clazz, sql, condition);
        if (null != list && list.size() > 0) {
            return list.get(0);
        } else {
            return null;
        }
    }

    /**
     * 自定义SQL查询
     * 该方法仅用于复杂sql
     *
     * @param clazz     数据类型
     * @param sql       自定义查询的SQL
     * @param condition 查询条件
     * @param <T>       数据类型
     * @return 查询结果集
     */
    public <T> List<T> customSelectList(Class<T> clazz, String sql, Object... condition) {
        List<T> result = new ArrayList<>();
        Field[] fields = this.getAllFields(clazz);
        Map<String, Object> map;
        rs.executeQuery(sql, condition);
        while (rs.next()) {
            map = new LinkedHashMap<>();
            String[] columnNames = rs.getColumnName();
            for (Field field : fields) {
                String fieldName = this.getFieldName(field);
                for (String columnName : columnNames) {
                    if (columnName.toLowerCase().equals(fieldName.toLowerCase())) {
                        map.put(field.getName(), rs.getString(fieldName));
                    }
                }
            }
            result.add(JSON.parseObject(JSON.toJSONString(map), clazz));
        }
        return result;
    }

    /**
     * 自定义查询(查询一条记录)
     *
     * @param executeSql 查询sql
     * @param condition 查询条件
     * @return 查询结果
     */
    public Map<String, String> customSelectOne(String executeSql, Object... condition) {
        return customSelectList(executeSql, condition).get(0);
    }

    /**
     * 自定义查询
     *
     * @param executeSql 查询sql
     * @param condition 查询条件
     * @return 查询结果
     */
    public List<Map<String, String>> customSelectList(String executeSql, Object... condition) {
        List<Map<String, String>> list = new ArrayList<>();
        rs.executeQuery(executeSql, condition);
        Map<String, String> map;
        while (rs.next()) {
            map = new HashMap<>();
            String[] columnNames = rs.getColumnName();
            for (String columnName : columnNames) {
                map.put(columnName, rs.getString(columnName));
            }
            list.add(map);
        }
        return list;
    }

    /**
     * 保存数据
     *
     * @param object 数据实体
     * @return 是否保存成功
     */
    public boolean save(Object object) {
        if (null == object) {
            return false;
        }
        List<Object> list = new ArrayList<>();
        list.add(object);
        return this.batchSave(list);
    }

    /**
     * 批量保存
     *
     * @param objectList 数据实体集
     * @return 是否保存成功
     */
    public boolean batchSave(List<Object> objectList) {
        if (null == objectList || objectList.size() == 0) {
            return false;
        }
        Class<?> clazz = objectList.get(0).getClass();
        String tableName = this.getTableName(clazz);
        StringBuilder preSql = new StringBuilder("insert into ");
        preSql.append(tableName).append("(").append(this.generateInsertColumnString(clazz)).append(")");
        StringBuilder sql = new StringBuilder(preSql).append(" values ");
        for (Object object : objectList) {
            Field[] entityFields = this.getAllFields(object.getClass());
            StringBuilder batchSql = new StringBuilder("(");
            for (Field field : entityFields) {
                Object value = this.getFieldValue(object, field.getName());
                if (ObjectUtil.isNull(value)) {
                    batchSql.append("null, ");
                } else {
                    batchSql.append(generateInsertFieldSql(value)).append(", ");
                }
            }
            sql.append(batchSql.substring(0, batchSql.length() - 2)).append("), ");
        }
        String executeSql = sql.substring(0, sql.length() - 2);
        return rs.execute(executeSql);
    }

    /**
     * 保存数据(可建权)
     *
     * @param entity     实体类
     * @param formModeId 模块ID(保存后数据显示在哪个模块中)
     * @param user       操作的用户
     * @return 调用保存接口后的返回值
     */
    @SuppressWarnings({"unused", "unchecked"})
    public String insert(Object entity, int formModeId, User user) {
        Map<String, Object> map = (Map<String, Object>) JSONObject.toJSON(entity);
        Field[] fields = getAllFields(entity.getClass());
        List<String> removeKeyList = new ArrayList<>();
        Map<String, Object> addMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            if (ObjectUtil.isEmpty(entry.getValue())) {
                removeKeyList.add(key);
                continue;
            }
            for (Field field : fields) {
                ColumnName annotation = field.getAnnotation(ColumnName.class);
                if (key.equals(field.getName())) {
                    if (null != annotation) {
                        removeKeyList.add(key);
                        addMap.put(annotation.value(), entry.getValue());
                    }
                }
            }
        }
        for (String key : removeKeyList) {
            map.remove(key);
        }
        for (Map.Entry<String, Object> entry : addMap.entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
        Map<String, String> saveData = new HashMap<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            saveData.put(entry.getKey(), String.valueOf(entry.getValue()));
        }
        return new ModeServiceUtil().addMoidfyModeData(user.getUID(), formModeId, 0, false, saveData, null);
    }

    /**
     * 更新数据
     *
     * @param entity 数据实体(ID字段不能为空)
     * @return 是否更新成功
     */
    @SuppressWarnings({"unused", "unchecked"})
    public boolean updateById(Object entity) {
        if (null == entity) {
            return false;
        }
        Map<String, Object> map = (Map<String, Object>) JSONObject.toJSON(entity);
        String id = map.get("id").toString();
        if (StringUtil.isEmpty(id)) {
            throw new ECException("id字段为空");
        }
        Class<?> clazz = entity.getClass();
        String tableName = this.getTableName(clazz);
        Field[] fields = getAllFields(clazz);
        List<String> removeKeyList = new ArrayList<>();
        Map<String, Object> addMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            if (ObjectUtil.isEmpty(entry.getValue()) || "id".equals(key)) {
                removeKeyList.add(key);
                continue;
            }
            for (Field field : fields) {
                ColumnName annotation = field.getAnnotation(ColumnName.class);
                if (key.equals(field.getName())) {
                    if (null != annotation) {
                        removeKeyList.add(key);
                        addMap.put(annotation.value(), this.generateValue(entry.getValue()));
                    }
                }
            }
        }
        for (String key : removeKeyList) {
            map.remove(key);
        }
        for (Map.Entry<String, Object> entry : addMap.entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
        StringBuilder whereKey = new StringBuilder();
        List<Object> whereList = new ArrayList<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            whereKey.append(", ").append(entry.getKey()).append(" = ?");
            whereList.add(entry.getValue());
        }
        whereList.add(id);
        String executeSql = "update " + tableName + " set id = " + id + whereKey.toString() + " where id = ?";
        return rs.executeUpdate(executeSql, whereList);
    }

    /**
     * 根据条件删除
     *
     * @param condition 删除的条件(实体类)
     * @return 是否删除成功
     */
    public boolean delete(Object condition) {
        if (null == condition) {
            return false;
        }
        Class<?> clazz = condition.getClass();
        String tableName = this.getTableName(clazz);
        Field[] fields = this.getAllFields(clazz);
        StringBuilder sql = new StringBuilder("delete from " + tableName + " where 1 = 1");
        List<Object> conditionList = new ArrayList<>();
        for (Field field : fields) {
            String fieldName = this.getFieldName(field);
            Object fieldValue = this.getFieldValue(condition, field.getName());
            if (null != fieldValue) {
                sql.append(" and ").append(fieldName).append(" = ?");
                conditionList.add(fieldValue);
            }
        }
        if (conditionList.size() == 0) {
            log.error("不能清空表数据[删除条件中未包含任何值]");
            throw new ECException("不能清空表数据[删除条件中未包含任何值]");
        }
        return rs.executeUpdate(sql.toString(), conditionList);
    }

    /**
     * 获取映射表名
     *
     * @param mappingName 映射名(对应workflow_mapping表中的name字段)
     * @return 表名
     */
    public static String getMappingTableName(String mappingName) {
        RecordSet recordSet = new RecordSet();
        recordSet.executeQuery("select * from workflow_mapping where action = 1 and name = ?", mappingName);
        if (recordSet.next()) {
            return recordSet.getString("table_name");
        } else {
            return null;
        }
    }

    /**
     * 获取映射流程ID
     *
     * @param mappingName 映射名(对应workflow_mapping表中的name字段)
     * @return 流程ID
     */
    public static String getMappingWorkflowId(String mappingName) {
        RecordSet recordSet = new RecordSet();
        recordSet.executeQuery("select * from workflow_mapping where action = 1 and name = ?", mappingName);
        if (recordSet.next()) {
            return recordSet.getString("workflow_id");
        } else {
            return null;
        }
    }

    // 以下方法为弃用的方法（慎用）——————————————————————————————————————————————————————————————————————————————————————————

    /**
     * 查询一条记录
     *
     * @param condition 查询条件(可以为空)
     * @param clazz     返回结果的数据类型
     * @return 数据实体
     */
    @Deprecated
    public <T> T selectOne(Map<String, Object> condition, Class<T> clazz) {
        List<T> list = selectList(condition, clazz);
        if (list.size() > 0) {
            return list.get(0);
        } else {
            return null;
        }
    }

    /**
     * 查询列表
     *
     * @param condition 查询条件(可以为空)
     * @param clazz     返回结果的数据类型
     * @return 数据列表
     */
    @Deprecated
    public <T> List<T> selectList(Map<String, Object> condition, Class<T> clazz) {
        List<T> result = new ArrayList<>();
        TableName annotation = clazz.getAnnotation(TableName.class);
        if (null == annotation) {
            throw new ECException(clazz.getName() + " 类中没有添加TableName注解");
        }
        String tableName = annotation.value();
        StringBuilder whereKey = new StringBuilder();
        List<Object> whereValue = new ArrayList<>();
        if (ObjectUtil.isNotNull(condition)) {
            for (Map.Entry<String, Object> entry : condition.entrySet()) {
                whereKey.append(" and ").append(entry.getKey()).append(" = ?");
                whereValue.add(entry.getValue());
            }
        }
        String executeSql = "select * from " + tableName + " where 1 = 1 " + whereKey.toString();
        rs.executeQuery(executeSql, whereValue);
        Field[] fields = getAllFields(clazz);
        Map<String, Object> map;
        while (rs.next()) {
            map = new LinkedHashMap<>();
            String[] columnNames = rs.getColumnName();
            for (Field field : fields) {
                String fieldName;
                if (null != field.getAnnotation(ColumnName.class)) {
                    fieldName = field.getAnnotation(ColumnName.class).value();
                } else {
                    fieldName = field.getName();
                }
                for (String columnName : columnNames) {
                    if (columnName.toLowerCase().equals(fieldName.toLowerCase())) {
                        map.put(field.getName(), rs.getString(fieldName));
                    }
                }
            }
            result.add(JSON.parseObject(JSON.toJSONString(map), clazz));
        }
        return result;
    }

    // 以下为私有方法—————————————————————————————————————————————————————————————————————————————————————————————————————

    /**
     * 获取表名
     *
     * @param clazz 类类型
     * @return 表名
     */
    private String getTableName(Class<?> clazz) {
        TableName tableNameAnnotation = clazz.getAnnotation(TableName.class);
        if (null != tableNameAnnotation) {
            return tableNameAnnotation.value();
        } else {
            TableMapping workflowMapping = clazz.getAnnotation(TableMapping.class);
            if (null != workflowMapping) {
                String mappingName = workflowMapping.value();
                String tableName = getMappingTableName(mappingName);
                if (null != tableName) {
                    return tableName;
                } else {
                    log.info("注解[WorkflowMapping]的值[" + mappingName + "]没有查询到数据");
                    throw new ECException("注解[WorkflowMapping]的值[" + mappingName + "]没有查询到数据");
                }
            } else {
                log.info("CLASS【" + clazz.getTypeName() + "】中未没有包含注解[TableName]或[WorkflowMapping]");
                throw new ECException("CLASS【" + clazz.getTypeName() + "】中未没有包含注解[TableName]或[WorkflowMapping]");
            }

        }
    }

    /**
     * 获取本类及父类的所有属性
     *
     * @param clazz class类型
     * @return 本类及父类的所有字段
     */
    private Field[] getAllFields(Class<?> clazz) {
        List<Field> fieldList = new ArrayList<>();
        while (clazz != null) {
            fieldList.addAll(new ArrayList<>(Arrays.asList(clazz.getDeclaredFields())));
            clazz = clazz.getSuperclass();
        }
        Field[] fields = new Field[fieldList.size()];
        return fieldList.toArray(fields);
    }

    /**
     * 获取字段名
     *
     * @param field 字段信息
     * @return 字段名
     */
    private String getFieldName(Field field) {
        ColumnName annotation = field.getAnnotation(ColumnName.class);
        if (null != annotation) {
            return annotation.value();
        } else {
            return field.getName();
        }
    }

    /**
     * 获取字段值
     *
     * @param object    实体类
     * @param fieldName 字段名
     * @return 字段值
     */
    private Object getFieldValue(Object object, String fieldName) {
        if (ObjectUtil.isNull(object)) {
            return null;
        }
        Class<?> clazz = object.getClass();
        String upperChar = fieldName.substring(0, 1).toUpperCase();
        String anotherStr = fieldName.substring(1);
        String methodName = "get" + upperChar + anotherStr;
        Method method = null;
        try {
            method = clazz.getMethod(methodName);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
        assert method != null;
        method.setAccessible(true);
        Object resultValue = null;
        try {
            resultValue = method.invoke(object);
        } catch (IllegalAccessException | InvocationTargetException e) {
            e.printStackTrace();
        }
        return resultValue;
    }

    /**
     * 生成insert语句的字段名字符串
     *
     * @param clazz 实体类class
     * @return sql
     */
    private String generateInsertColumnString(Class<?> clazz) {
        Field[] fields = getAllFields(clazz);
        List<String> columnNames = new ArrayList<>();
        for (Field field : fields) {
            ColumnName columnNameAnnotation = field.getAnnotation(ColumnName.class);
            if (null != columnNameAnnotation) {
                columnNames.add(columnNameAnnotation.value());
            } else {
                columnNames.add(field.getName());
            }
        }
        return String.join(",", columnNames);
    }

    private String generateValue(Object value) {
        String result;
        if (value instanceof Date) {
            Date date = (Date) value;
            result = DateUtil.format(date, "yyyy-MM-dd HH:mm:ss");
        } else {
            result = value.toString();
        }
        return result;
    }

    private String generateInsertFieldSql(Object value) {
        String result;
        if (value instanceof Date) {
            Date date = (Date) value;
            result = "'" + DateUtil.format(date, "yyyy-MM-dd HH:mm:ss") + "'";
        } else {
            result = "'" + value + "'";
        }
        return result;
    }


}
