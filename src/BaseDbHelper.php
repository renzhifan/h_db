<?php

namespace Hhz\DB;

use Medoo\Medoo;

/**
 *
 * @author Luomo
 *
 */
class BaseDbHelper
{
    const API_CODE_EXCEPTION = 5;//接口异常 不可预测失败
    // mysql默认端口
    const _DEFAULE_MYSQL_PORT_ = 3306;
    // mysql配置文件名
    const _CONFIG_FILE_ = '';
    // mysql配置字段
    const _CONFIG_SELECT_ = '';
    // 数据库名
    const _DATABASE_ = '';
    //表名
    const _TABLE_NAME = '';
    // 日志最大行数
    const _LOG_MAX_NUM_ = 1000;

    //最大重连次数
    const _MAX_RETRY_CONN = 10;

    const _METHOD_DELIMITER_ = '_';

    const _TIME_OUT_ = 1;

    const COLUMN_DATE_TIME_DEFAULE = '1970-01-01 00:00:00';

    protected static $tableName = "";

    private static $singleton = [];

    protected static $lastQuerySingletonKey = "";

    protected static $sqlLogs = [];

    protected static $connectName = '';

    protected static $writeFuncitonList = [
        "insert"        => true,
        "update"        => true,
        "delete"        => true,
        "replace"       => true,
        "id"            => true,
        "select_master" => true,
        "get_master"    => true,
        "has_master"    => true,
        "count_master"  => true,
        "max_master"    => true,
        "min_master"    => true,
        "avg_master"    => true,
        "sum_master"    => true
    ];

    protected static $readFunctionList = [
        "select" => true,
        "get"    => true,
        "has"    => true,
        "count"  => true,
        "max"    => true,
        "min"    => true,
        "avg"    => true,
        "sum"    => true,
        "rand"   => true,
    ];

    protected static $transactionFunctionList = [
        "action"       => true,
        "query_master" => true,
    ];

    protected static $debugFunctionList = [
        "debug" => true,
        "error" => true,
        "log"   => true,
        "last"  => true,
        "query" => true,
        "id"    => true
    ];

    protected static function getConnection($master, $force = false)
    {
        static::$connectName = $singletonKey = static::_DATABASE_ . static::_CONFIG_SELECT_ . ":" . (int)$master;

        static::$lastQuerySingletonKey = $singletonKey;
        if (isset(self::$singleton[$singletonKey])) {
            if (count(self::$singleton[$singletonKey]->log()) > static::_LOG_MAX_NUM_) {
                unset(self::$singleton[$singletonKey]);
            }
        }
        if (!isset(self::$singleton[$singletonKey]) || $force) {
            $dbConfig = new IniConfig();
            $dbConfig =$dbConfig->getDatabaseConfig();
            try {
                Timer::start('connect');
                self::$singleton[$singletonKey] = new MedooScrew([
                    'database_type' => 'mysql',
                    'charset'       => $dbConfig["charset"],
                    'database_name' => $dbConfig["db"],
                    'server'        => $dbConfig["host"],
                    'username'      => $dbConfig["user"],
                    'password'      => $dbConfig["passwd"],
                    'port'          => isset($dbConfig["port"]) ? $dbConfig["port"] : self::_DEFAULE_MYSQL_PORT_,
                    'option'        => [
                        \PDO::ATTR_TIMEOUT => self::_TIME_OUT_,
                    ]
                ]);
                $time = Timer::stop('connect', Timer::UNIT_MS);
                if ($time > 300) {
                    Log::db_connectslow_warning(static::$connectName . "-connectTime:" . $time . "ms",
                        [
                            $dbConfig,
                            self::$singleton[$singletonKey]->info()
                        ]);
                }
            } catch (\Exception $e) {
                Log::db_error_error(static::_CONFIG_FILE_ . '#' . static::_CONFIG_SELECT_ . '#' . $e->getMessage(),
                    $dbConfig);
                throw new \Exception("mysql connect error", self::API_CODE_EXCEPTION);
            }
        }

        //因为只有命令行，并且是daemon程序才会有大概率的重连问题，为了降低风险，暂时只有cli模式支持重连
//        if (HhzEnv::isCli()) {
//
//            if (!empty(self::$singleton[$singletonKey])) {
//                try {
//                    $info = self::$singleton[$singletonKey]->info();
//                    if ($info['server'] === false) {
//                        Log::db_error_error(static::_CONFIG_FILE_ . '#' . static::_CONFIG_SELECT_ . '#' . "数据库因为其他原因连不上，进行重连",
//                            $info);
//                        //每次重连，等待50ms
//                        usleep(50000);
//                        return self::getConnection($master, true);
//                    }
//                } catch (\PDOException $e) {
//
//                }
//            }
//        }
        return self::$singleton[$singletonKey];
    }

    public static function __callstatic($method, $args)
    {
        if (isset(self::$readFunctionList[$method])) {
            /**
             * @var Medoo $medoo
             */
            $medoo = self::getConnection(false);
            $medoo->getTableColumns(static::$tableName);
            array_unshift($args, static::$tableName);
        } elseif (isset(self::$writeFuncitonList[$method])) {
            static::$tableName = preg_replace('/\([a-zA-Z]*\)/', '', static::$tableName);
            $medoo = self::getConnection(true);
            $medoo->getTableColumns(static::$tableName);
            array_unshift($args, static::$tableName);
            $methods = explode(self::_METHOD_DELIMITER_, $method);
            if (count($methods) == 2) {
                $method = $methods[0];
            }
        } elseif (isset(self::$debugFunctionList[$method])) {
            if ($method == 'query') {
                $medoo = self::getConnection(false);
            } elseif (isset(self::$singleton[self::$lastQuerySingletonKey])) {
                $medoo = self::$singleton[self::$lastQuerySingletonKey];
            }
            $medoo->getTableColumns();
        } elseif (isset(self::$transactionFunctionList[$method])) {
            $medoo = self::getConnection(true);
            $medoo->getTableColumns();
            $methods = explode(self::_METHOD_DELIMITER_, $method);
            if (count($methods) == 2) {
                $method = $methods[0];
            }
        } else {
            throw new \Exception("use undefined Medoo function:{$method} in " . static::$tableName . " with params " . json_encode($args), self::API_CODE_EXCEPTION);
        }
        Timer::start('query');
        $result = call_user_func_array([
            $medoo,
            $method
        ], $args);
        $errorInfo = null;
        if ($result instanceof \PDOStatement) {
            $errorInfo = $result->errorInfo();
        } elseif ($result === false) {
            $errorInfo = $medoo->error();
        }
        if ($errorInfo) {
            if ($errorInfo[0] != '00000' || $errorInfo[1] !== null) {
                $sql = $medoo->last();
                Log::db_error_error($sql, [
                    static::$connectName,
                    $method,
                    $args,
                    $errorInfo
                ]);
            }
        }
        $time = Timer::stop('query', Timer::UNIT_MS);
        if ($time > 300) {
            $sql = $medoo->last();
            Log::db_queryslow_warning(static::$connectName . "-queryTime:" . $time . "ms",
                [
                    $sql,
                    $medoo->info(),
                    $method,
                    $args
                ]);
        }
//        if (HhzEnv::isShopTest() || HhzEnv::isShopHaoHaoCe() || HhzEnv::isCli()) {
//            self::addLogs($medoo, $method, $result);
//        }

        return $result;
    }

    private static function addLogs($medoo, $method, $result)
    {
        if (!key_exists($method,
                array_merge(self::$transactionFunctionList, self::$debugFunctionList)) && $method != 'id') {
            if (count(self::$sqlLogs) < 100) {
                $rows = method_exists($result, 'rowCount') ? $result->rowCount() : '无';

                self::$sqlLogs[] = vsprintf('%s  %s  %s    %s     %s     rows:%s', [
                    date('Y-m-d H:i:s'),
                    static::$connectName,
                    $method,
                    !is_null($medoo) ? $medoo->last() : '',
                    !is_null($medoo) ? json_encode($medoo->error()) : '',
                    $rows
                ]);
                if (count(self::$sqlLogs) > 50) {
                    array_shift(self::$sqlLogs);
                }
            }
        }
    }

    public static function sqlLogs()
    {
        $format = PHP_SAPI == 'cli' ? "\n" : "\n";
        return implode($format, self::$sqlLogs);
    }

    public static function clearLogs()
    {
        self::$sqlLogs = [];
    }
}

