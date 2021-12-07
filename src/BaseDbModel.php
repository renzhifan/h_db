<?php

namespace Hhz\DB;


/**
 *
 * @author Luomo
 * @method static array action(callable $actions)
 * @method static array log()
 * @method static \PDOStatement|bool query($query, $map = [])
 * @method static array|null error()
 * @method static array|mixed get($field, $where = [])
 * @method static array|bool select($field, $where = [])
 * @method static array|bool select_master($field, $where = [])
 * @method static bool|\PDOStatement update($data, $where = [])
 * @method static \PDOStatement|bool insert($data)
 * @method static \PDOStatement|bool delete($where = [])
 * @method static int|mixed|string id()
 * @method static bool|int|mixed|string max($column, $where = [])
 * @method static bool|int|mixed|string min($column, $where = [])
 * @method static bool|int|mixed|string count($where = [])
 */
class BaseDbModel extends BaseDbHelper
{
    // 是否分表
    const _SUB_TABLE = false;
    // 分表数量
    const _SUB_TABLE_NUM = 1;
    // 分表方法
    const _SUB_TABLE_FUNCTION = "crc32Hash";
    // 分表辅助参数
    const _SUB_TABLE_DATE_FORMAT = "Ym";
    const _SUB_TABLE_DAY_FORMAT = 'Ymd';

    // 分表字段
    const _SUB_TABLE_FIELD = null;
    // 辅助分表字段
    const _SUB_TABLE_ASSIST_FIELD = null;
    // 辅助分表字段转换为分表字段方法
    const _SUB_TABLE_ASSIST_FUNCTION = null;
    // 表名前缀
    const _TABLE_NAME = null;
    // 表名分隔符
    const _TABLE_NAME_SEPARATOR = "_";
    //
    const STATUS_NORMAL = 1;
    // 状态为删除
    const STATUS_DELETE = 9;

    protected static $enforceSubTable = "";

    public static function __callstatic($method, $args)
    {
        if (static::_SUB_TABLE) {
            if (static::$enforceSubTable || isset(self::$transactionFunctionList[$method])) {
                return parent::__callstatic($method, $args);
            } else {
                // 开启分表 使用分表查询方法
                return self::subTablesQuery($method, $args);
            }
        } else {
            // 单表查询 直接运行
            self::setTableName(static::getTableName($method, $args));
            return parent::__callstatic($method, $args);
        }
    }

    public static function getTableName($method = '', $args = [])
    {
        return static::_TABLE_NAME;
    }

    public static function enforceSubTableNameSuffix($tableSuffix)
    {
        $tableName = static::getTableName() . static::_TABLE_NAME_SEPARATOR . $tableSuffix;
        self::setTableName($tableName);
        static::$enforceSubTable = true;
    }

    public static function enforceSubTableNameBySubKey($subKey = null)
    {
        $tableName = static::getSubTableName($subKey);
        self::setTableName($tableName);
        static::$enforceSubTable = true;
        return $tableName;
    }

    public static function resetEnforceSubTable()
    {
        self::setTableName(null);
        static::$enforceSubTable = false;
    }

    protected static function setTableName($tableName)
    {
        static::$tableName = $tableName;
    }

    /**
     * 分表查询
     *
     * @param string $method
     *            方法名
     * @param array $args
     *            调用参数
     * @return array $result 查询结果
     * @throws \Exception
     */
    private static function subTablesQuery($method, $args)
    {

        if ($method == "insert") {
            // 插入操作
            if (isset($args[0])) {
                $classifyData = self::classifyInsertData($args[0]);
                return self::subTableExe($method, $classifyData);
            } else {
                throw new \Exception("insert wrong args", ApiCode::API_CODE_EXCEPTION);
            }
        } elseif (isset(self::$debugFunctionList[$method])) {
            return parent::__callstatic($method, $args);
        } else {
            // 其他带where的操作
            $classifyData = self::classifyWhere($args);
            return self::subTableExe($method, $classifyData);
        }
    }

    /**
     * 执行归类后的执行结果并返回合并数据
     *
     * @param string $method
     * @param array $classifyData
     * @return array 执行结果
     */
    private static function subTableExe($method, $classifyData)
    {
        switch ($method) {
            // 返回单条bool数据
            case "insert":
            case "update":
            case "delete":
            case "replace":
            case "has":
                $result = false;
                foreach ($classifyData as $tableName => $args) {
                    self::setTableName($tableName);
                    $result = $result || parent::__callstatic($method, $args);
                }
                break;
            case "has_master":
                $result = false;
                foreach ($classifyData as $tableName => $args) {
                    self::setTableName($tableName);
                    $result = $result || parent::__callstatic($method, $args);
                }
                break;
            // 返回单条数据
            case "get":
                $result = null;
                foreach ($classifyData as $tableName => $args) {
                    self::setTableName($tableName);
                    $result = parent::__callstatic($method, $args);
                    if ($result) {
                        break;
                    }
                }
                break;
            case "get_master":
                $result = null;
                foreach ($classifyData as $tableName => $args) {
                    self::setTableName($tableName);
                    $result = parent::__callstatic($method, $args);
                    if ($result) {
                        break;
                    }
                }
                break;
            // 返回总数
            case "count":
                $result = 0;
                foreach ($classifyData as $tableName => $args) {
                    self::setTableName($tableName);
                    $result += parent::__callstatic($method, $args);
                }
                break;
            case "count_master":
                $result = 0;
                foreach ($classifyData as $tableName => $args) {
                    self::setTableName($tableName);
                    $result += parent::__callstatic($method, $args);
                }
                break;
            // 返回最大值
            case "max":
                $result = PHP_INT_MIN;
                foreach ($classifyData as $tableName => $args) {
                    self::setTableName($tableName);
                    $result = max($result, parent::__callstatic($method, $args));
                }
                break;
            // 返回最大值
            case "min":
                $result = PHP_INT_MAX;
                foreach ($classifyData as $tableName => $args) {
                    self::setTableName($tableName);
                    $result = min($result, parent::__callstatic($method, $args));
                }
                break;
            case "avg":
                if (count($classifyData) > 0) {
                    throw new \Exception("sub table can't use avg", ApiCode::API_CODE_EXCEPTION);
                } else {
                    foreach ($classifyData as $tableName => $args) {
                        self::setTableName($tableName);
                        $result = parent::__callstatic($method, $args);
                    }
                    break;
                }
            case "sum":
                $result = 0;
                foreach ($classifyData as $tableName => $args) {
                    self::setTableName($tableName);
                    $result += parent::__callstatic($method, $args);
                }
                break;
            // 返回多条数据
            case "select":
                $result = [];
                foreach ($classifyData as $tableName => $args) {
                    self::setTableName($tableName);
                    $result = array_merge($result, (array)parent::__callstatic($method, $args));
                }
                break;
            case "select_master":
                $result = [];
                foreach ($classifyData as $tableName => $args) {
                    self::setTableName($tableName);
                    $result = array_merge($result, (array)parent::__callstatic($method, $args));
                }
                break;
        }
        return $result;
    }

    /**
     * 归类执行语句
     *
     * @param array $args
     * @return array 归类结果
     * @throws \Exception
     */
    private static function classifyWhere($args)
    {
        $where = array_pop($args);
        $classifyData = [];
        $findSubTableField = false;
        if (isset($where[static::_SUB_TABLE_FIELD])) {
            $findSubTableField = true;
            // 简单查询且已设置分表字段
            $classifyWhere = self::classifyWhereBySubField($where[static::_SUB_TABLE_FIELD]);
            foreach ($classifyWhere as $tableName => $tempWhere) {
                $where[static::_SUB_TABLE_FIELD] = $tempWhere;
                $classifyData[$tableName] = array_merge($args, [
                    $where
                ]);
            }
        } else {
            if (is_array($where)) {
                $whereKeys = array_keys($where);
                $whereAND = preg_grep("/^AND\s*#?$/i", $whereKeys);
            }
            if (!empty($whereAND)) {
                // 复杂查询
                foreach ($whereAND as $andKey) {
                    if (isset($where[$andKey][static::_SUB_TABLE_FIELD])) {
                        // 已设置分表字段
                        $findSubTableField = true;
                        $classifyWhere = self::classifyWhereBySubField($where[$andKey][static::_SUB_TABLE_FIELD]);
                        foreach ($classifyWhere as $tableName => $tempWhere) {
                            $where[$andKey][static::_SUB_TABLE_FIELD] = $tempWhere;
                            $classifyData[$tableName] = array_merge($args, [
                                $where
                            ]);
                        }
                    }
                }
            }
        }
        if (!$findSubTableField) {
            // 未找到分表字段，尝试使用辅助分表字段
            if (static::_SUB_TABLE_ASSIST_FIELD && static::_SUB_TABLE_ASSIST_FUNCTION) {
                if (isset($where[static::_SUB_TABLE_ASSIST_FIELD])) {
                    $findSubTableField = true;
                    $classifyWhere = self::classifyWhereByAssistSubField($where[static::_SUB_TABLE_ASSIST_FIELD]);
                    foreach ($classifyWhere as $tableName => $tempWhere) {
                        $where[static::_SUB_TABLE_FIELD] = $tempWhere[static::_SUB_TABLE_FIELD];
                        $where[static::_SUB_TABLE_ASSIST_FIELD] = $tempWhere[static::_SUB_TABLE_ASSIST_FIELD];
                        $classifyData[$tableName] = array_merge($args, [
                            $where
                        ]);
                    }
                } else {
                    $whereKeys = array_keys($where);
                    $whereAND = preg_grep("/^AND\s*#?$/i", $whereKeys);
                    if (!empty($whereAND)) {
                        // 复杂查询
                        foreach ($whereAND as $andKey) {
                            if (isset($where[$andKey][static::_SUB_TABLE_ASSIST_FIELD])) {
                                // 已设置分表字段
                                $findSubTableField = true;
                                $classifyWhere = self::classifyWhereByAssistSubField($where[$andKey][static::_SUB_TABLE_ASSIST_FIELD]);
                                foreach ($classifyWhere as $tableName => $tempWhere) {
                                    $where[$andKey][static::_SUB_TABLE_FIELD] = $tempWhere[static::_SUB_TABLE_FIELD];
                                    $where[$andKey][static::_SUB_TABLE_ASSIST_FIELD] = $tempWhere[static::_SUB_TABLE_ASSIST_FIELD];
                                    $classifyData[$tableName] = array_merge($args, [
                                        $where
                                    ]);
                                }
                            }
                        }
                    } else {
                        Log::db_subtable_error(static::_TABLE_NAME, $where);
                        throw new \Exception("sub table faild", ApiCode::API_CODE_EXCEPTION);
                    }
                }
                if (!$findSubTableField) {
                    Log::db_subtable_error(static::_TABLE_NAME, $where);
                    throw new \Exception("sub table faild", ApiCode::API_CODE_EXCEPTION);
                }
            } else {
                Log::db_subtable_error(static::_TABLE_NAME, $where);
                throw new \Exception("sub table faild", ApiCode::API_CODE_EXCEPTION);
            }
        }
        return $classifyData;
    }

    private static function classifyWhereByAssistSubField($assistSubField)
    {
        $classifyWhere = [];
        if (is_array($assistSubField)) {
            foreach ($assistSubField as $value) {
                $subFeildValue = call_user_func_array(static::_SUB_TABLE_ASSIST_FUNCTION, [
                    $value
                ]);
                $tableName = static::getSubTableName($subFeildValue);
                $classifyWhere[$tableName][static::_SUB_TABLE_FIELD][] = $subFeildValue;
                $classifyWhere[$tableName][static::_SUB_TABLE_ASSIST_FIELD][] = $value;
            }
            foreach ($classifyWhere as $tableName => $where) {
                $classifyWhere[$tableName][static::_SUB_TABLE_FIELD] = array_values(array_unique($where[static::_SUB_TABLE_FIELD]));
                $classifyWhere[$tableName][static::_SUB_TABLE_ASSIST_FIELD] = array_values(array_unique($where[static::_SUB_TABLE_ASSIST_FIELD]));
            }
        } else {
            $subFeildValue = call_user_func_array(static::_SUB_TABLE_ASSIST_FUNCTION, [
                $assistSubField
            ]);
            $tableName = static::getSubTableName($subFeildValue);
            $classifyWhere[$tableName][static::_SUB_TABLE_FIELD] = $subFeildValue;
            $classifyWhere[$tableName][static::_SUB_TABLE_ASSIST_FIELD] = $assistSubField;
        }
        return $classifyWhere;
    }

    /**
     * 使用主分表字段拆分查询语句
     *
     * @param array $otherArgs
     * @param array $subField
     * @return array
     */
    private static function classifyWhereBySubField($subField)
    {
        $classifyWhere = [];
        if (is_array($subField)) {
            foreach ($subField as $value) {
                if ($value) {
                    $tableName = static::getSubTableName($value);
                    $classifyWhere[$tableName][] = $value;
                }
            }
        } else {
            $tableName = static::getSubTableName($subField);
            $classifyWhere[$tableName] = $subField;
        }
        return $classifyWhere;
    }

    private static function fliterWhere($args, $where)
    {
        if (is_array($where[static::_SUB_TABLE_FIELD])) {
            $classifyWhere = [];
            foreach ($where[static::_SUB_TABLE_FIELD] as $value) {
                $tableName = static::getSubTableName($value);
                $classifyWhere[$tableName][] = $value;
            }
            foreach ($classifyWhere as $tableName => $tempWhere) {
                $where[static::_SUB_TABLE_FIELD] = $tempWhere;
                $classifyData[$tableName] = $args + $where;
            }
        } else {
            $tableName = static::getSubTableName($where[static::_SUB_TABLE_FIELD]);
            $classifyData[$tableName] = $args + $where;
        }
        return $classifyData;
    }

    /**
     * 归类插入数据
     *
     * @param array $data
     * @return array
     * @throws \Exception
     */
    private static function classifyInsertData($data)
    {
        if (!isset($data[0])) {
            // 将单条插入默认转化为多条插入处理
            $datas = [
                $data
            ];
        }
        $classifyData = [];
        foreach ($datas as $data) {
            if (isset($data[static::_SUB_TABLE_FIELD])) {
                $tableName = static::getSubTableName($data[static::_SUB_TABLE_FIELD]);
                $classifyData[$tableName][] = [
                    $data
                ];
            } else {
                Log::db_subtable_error(static::$tableName, $data);
                throw new \Exception("insert data don't have sub table field", ApiCode::API_CODE_EXCEPTION);
            }
        }
        return $classifyData;
    }

    /**
     * 获取分表名
     *
     * @param string $subKey
     * @return string
     * @throws \Exception
     */
    public static function getSubTableName($subKey = null)
    {
        if (self::_SUB_TABLE_FUNCTION) {
            if (method_exists(__CLASS__, static::_SUB_TABLE_FUNCTION)) {
                $tableSuffix = call_user_func_array([
                    'static',
                    static::_SUB_TABLE_FUNCTION
                ], [
                    $subKey
                ]);
                return static::getTableName() . static::_TABLE_NAME_SEPARATOR . $tableSuffix;
            } else {
                throw new \Exception("Sub table function is wrong!", ApiCode::API_CODE_EXCEPTION);
            }
        } else {
            throw new \Exception("Sub table function is not set!", ApiCode::API_CODE_EXCEPTION);
        }
    }

    private static function crc32Hash($hashKey)
    {
        if (isset($hashKey)) {
            return intval(sprintf("%u", crc32($hashKey)) / static::_SUB_TABLE_NUM) % static::_SUB_TABLE_NUM;
        } else {
            throw new \Exception("hashKey is empty!", ApiCode::API_CODE_EXCEPTION);
        }
    }

    private static function crc32MasterOfObjId($hashKey)
    {
        if (isset($hashKey)) {
            return intval(sprintf("%u",
                        crc32(self::getUidByObjId($hashKey))) / static::_SUB_TABLE_NUM) % static::_SUB_TABLE_NUM;
        } else {
            throw new \Exception("hashKey is empty!", ApiCode::API_CODE_EXCEPTION);
        }
    }

    private static function dayHash($hashKey)
    {
        if (!$hashKey) {
            $hashKey = time();
        }
        return date(static::_SUB_TABLE_DAY_FORMAT, $hashKey);
    }

    private static function getUidByObjId($obj_id)
    {
        $uid36 = substr($obj_id, -7);
        return base_convert($uid36, 36, 10);
    }

    private static function dateHash($hashKey)
    {
        if (!$hashKey) {
            $hashKey = time();
        }
        return date(static::_SUB_TABLE_DATE_FORMAT, $hashKey);
    }

    protected static function buidData($data)
    {
        if (static::$fields) {

            $data = array_filter($data, function ($key) {

                if (!isset(static::$fields[$key]) && !in_array($key, static::$fields)) {
                    return false;
                }

                return true;
            }, ARRAY_FILTER_USE_KEY);

            array_walk($data, function (&$v, $k) {
                $filters = isset(static::$fields[$k]) ? static::$fields[$k] : null;

                if (!is_null($filters)) {
                    foreach ($filters as $filter) {
                        $v = $filter($v);
                    }
                }
            });
        }

        return $data;


    }
}

