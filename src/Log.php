<?php

namespace Hhz\DB;

use Monolog\Formatter\LineFormatter;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;

/**
 * Class Log
 * @method static statsign_info($title, $data = [])
 * @method static cron_info($title, array $data = [])
 * @method static position_info($title, $data = [])
 * @method static error(string $message, array $data)
 * @method static info(string $message, array $data)
 * @method static debug(string $message, array $data)
 * @package Doraemon\tools
 */
class Log
{

    const DEFAULT_LOG_PATH_PREFIX = "/data/logs_bak/";

    const DEFAULT_LOG_DIRECTORY = "doraemon";

    const DEFAULE_PROJECT_DIRECTORY_PREFIX = 'g_';

    const DEFAULE_LOG_SEPARATOR = '#';

    private static $singleLog = [];

    private static $singleStreamHandler = [];

    private static $singleStreamHandlerPath = [];

    private static $singleFormatter = [];

    protected static $levels = [
        'debug'     => Logger::DEBUG,
        'info'      => Logger::INFO,
        'notice'    => Logger::NOTICE,
        'warning'   => Logger::WARNING,
        'error'     => Logger::ERROR,
        'critical'  => Logger::CRITICAL,
        'alert'     => Logger::ALERT,
        'emergency' => Logger::EMERGENCY
    ];

    private static $logSetting = [
        'logPath'   => "",
        'logSlice'  => 'daily',
        'expireDay' => 30,
        'logList'   => [
            'general'      => [
                "dir"      => "general/",
                "logLevel" => Logger::DEBUG
            ],
            'db'           => [
                "dir"      => "db/",
                "logLevel" => Logger::DEBUG
            ],
            'sys'          => [
                "dir"      => "sys/",
                "logLevel" => Logger::DEBUG
            ],
            'redis'        => [
                "dir"      => "redis/",
                "logLevel" => Logger::DEBUG
            ],
            'curl'         => [
                "dir"      => "curl/",
                "logLevel" => Logger::DEBUG
            ],
            'queue'        => [
                "dir"      => "queue/",
                "logLevel" => Logger::DEBUG
            ],
            'api'          => [
                "dir"      => "api/",
                "logLevel" => Logger::DEBUG
            ],
            'inner'        => [
                "dir"      => "inner/",
                "logLevel" => Logger::DEBUG
            ],
            'amqp'         => [
                "dir"      => "amqp/",
                "logLevel" => Logger::DEBUG
            ],
            'oss'          => [
                "dir"      => "oss/",
                "logLevel" => Logger::DEBUG
            ],
            'order'        => [
                "dir"      => "order/",
                "logLevel" => Logger::DEBUG
            ],
            'goods'        => [
                "dir"      => "goods/",
                "logLevel" => Logger::DEBUG
            ],
            'pay'          => [
                "dir"      => "pay/",
                "logLevel" => Logger::DEBUG
            ],
            'callback'     => [
                "dir"      => "callback/",
                "logLevel" => Logger::DEBUG
            ],
            'settlement'   => [
                "dir"      => "settlement/",
                "logLevel" => Logger::DEBUG
            ],
            'refund'       => [
                "dir"      => "refund/",
                "logLevel" => Logger::DEBUG
            ],
            'cart'         => [
                "dir"      => "cart/",
                "logLevel" => Logger::DEBUG
            ],
            'smallenergy'  => [
                "dir"      => "smallenergy/",
                "logLevel" => Logger::DEBUG
            ],
            'point'        => [
                "dir"      => "point/",
                "logLevel" => Logger::DEBUG
            ],
            'clearcache'   => [
                "dir"      => "clearcache/",
                "logLevel" => Logger::DEBUG
            ],
            'coupon'       => [
                "dir"      => "coupon/",
                "logLevel" => Logger::DEBUG
            ],
            'outcoupon'    => [
                "dir"      => "outcoupon/",
                "logLevel" => Logger::DEBUG
            ],
            'groupbuy'     => [
                "dir"      => "group_buy/",
                "logLevel" => Logger::DEBUG
            ],
            'event'        => [
                "dir"      => "event/",
                "logLevel" => Logger::DEBUG
            ],
            'kafka'        => [
                "dir"      => "kafka/",
                "logLevel" => Logger::DEBUG
            ],
            'erp'          => [
                "dir"      => "erp/",
                "logLevel" => Logger::DEBUG
            ],
            'essync'       => [
                "dir"      => "essync/",
                "logLevel" => Logger::DEBUG
            ],
            'delayedtask'  => [
                "dir"      => "delayedtask/",
                "logLevel" => Logger::DEBUG
            ],
            'wiki'         => [
                "dir"      => "wiki/",
                "logLevel" => Logger::DEBUG
            ],
            'designervote' => [
                "dir"      => "designervote/",
                "logLevel" => Logger::DEBUG
            ],
            'position'     => [
                "dir"      => "position/",
                "logLevel" => Logger::DEBUG
            ],
            'statsign'     => [
                "dir"      => "statsign/",
            ],
            'video'     => [
                "dir"      => "video/",
                "logLevel" => Logger::DEBUG
            ],
            'essearch'     => [
                "dir"      => "essearch/",
                "logLevel" => Logger::DEBUG
            ],
            'funeng'     => [
                "dir"      => "funeng/",
                "logLevel" => Logger::DEBUG
            ],
            'goword'=> [
                "dir"      => "goword/",
                "logLevel" => Logger::DEBUG
            ],
            'im'=> [
                "dir"      => "im/",
                "logLevel" => Logger::DEBUG
            ],
            'push'=> [
                "dir"      => "push/",
                "logLevel" => Logger::DEBUG
            ],
            'cron'     => [
                "dir"      => "cron/",
                "logLevel" => Logger::DEBUG
            ],
        ]
    ];

    private static function getSingleLog($logPath)
    {
        if (!isset(self::$singleLog[$logPath])) {
            self::$singleLog[$logPath] = new Logger($logPath);
        }
        return self::$singleLog[$logPath];
    }

    private static function getSlice()
    {
        switch (self::$logSetting['logSlice']) {
            case "hourly":
                $logSuffix = "-" . date("YmdH") . ".log";
                break;
            case "daily":
                $logSuffix = "-" . date("Ymd") . ".log";
                break;
        }
        return $logSuffix;
    }

    private static function setLogPath($backtrace = null)
    {
        if (isset($_SERVER['APP_NAME'])) {
            self::$logSetting['logPath'] = self::DEFAULT_LOG_PATH_PREFIX . $_SERVER['APP_NAME'] . DIRECTORY_SEPARATOR;
        } else {
            $backtraceDir = ltrim(str_replace(dirname(dirname(__DIR__)), '', $backtrace[0]['file']), '/');
            $dirList = explode("/", $backtraceDir);
            $logPath = str_replace(self::DEFAULE_PROJECT_DIRECTORY_PREFIX, '', array_shift($dirList));
            self::$logSetting['logPath'] = self::DEFAULT_LOG_PATH_PREFIX . $logPath . DIRECTORY_SEPARATOR;
        }
    }

    public static function setExcelPath($oldPath, $fileName)
    {
        if (isset($_SERVER['APP_NAME']) && $_SERVER['APP_NAME'] == 'sapi') {
            $path = self::DEFAULT_LOG_PATH_PREFIX . $_SERVER['APP_NAME'] . '/excel';
        } else {
            $path = self::DEFAULE_PROJECT_DIRECTORY_PREFIX . 'excel';
        }
        if (!file_exists($path)) {
            if (!mkdir($path, 0755, true)) {
                throw new \Exception('文件夹创建失败');
            }
        }
        $ret = move_uploaded_file($oldPath, $path . '/' . $fileName);
        if ($ret) {
            return $path . '/' . $fileName;
        }
    }

    public static function __callstatic($method, $args)
    {

        $backtrace = debug_backtrace(0, 1);
        self::setLogPath($backtrace);
        $methodList = explode("_", strtolower($method));
        if (count($methodList) > 1) {
            if (count($methodList) == 2) {
                self::addSystemLog($methodList[0], $methodList[1], $args);
            } elseif (count($methodList) > 2) {
                self::addSystemLog($methodList[0], $methodList[2], $args, $methodList[1]);
            }
        } else {
            list ($logPath, $line) = self::getBackTrace($backtrace);
            self::addGeneralLog($logPath, $line, $method, $args);
        }
    }

    private static function getBackTrace($backtrace)
    {
        $callFile = ltrim(str_replace(array(
            dirname(dirname(__DIR__)),
            '.php',
            '/'
        ), array(
            "",
            "",
            "-"
        ), $backtrace[0]['file']), "-");

        if (empty($callFile)) {
            $callFile = 'general';
        }
        return array(
            $callFile,
            $backtrace[0]['line']
        );
    }

    private static function addSystemLog($logTag, $logType, $logData, $logPrefix = null)
    {
        $logPath = $logPrefix ? $logTag . "-" . $logPrefix : $logTag;
        if (!isset(self::$singleFormatter[$logPath])) {
            $output = "%datetime%#%level_name%#%message%#%context%\n";
            self::$singleFormatter[$logPath] = new LineFormatter($output);
        }
        self::getSingleLog($logPath);
        $realPath = self::$logSetting['logPath'] . self::$logSetting['logList'][$logTag]['dir'] . $logPath . self::getSlice();
        if (!isset(self::$singleStreamHandlerPath[$logPath]) || self::$singleStreamHandlerPath[$logPath] != $realPath || !file_exists($realPath)) {
            self::$singleStreamHandler[$logPath] = new StreamHandler($realPath,
                self::$logSetting['logList'][$logTag]['logLevel'], true, 0666);
            self::$singleStreamHandler[$logPath]->setFormatter(self::$singleFormatter[$logPath]);
            //释放文件句柄
            if (!empty(self::$singleLog[$logPath]->getHandlers())) {
                self::$singleLog[$logPath]->popHandler()->close();
            }
            self::$singleLog[$logPath]->pushHandler(self::$singleStreamHandler[$logPath]);
            self::$singleStreamHandlerPath[$logPath] = $realPath;
        }
        self::writeLog($logPath, $logType, $logData);
    }

    private static function addGeneralLog($logPath, $line, $logType, $logData)
    {
        if (!isset(self::$singleFormatter[$logPath . "-" . $line])) {
            $output = "%datetime%#line:{$line}#%level_name%#%message%#%context%\n";
            self::$singleFormatter[$logPath . "-" . $line] = new LineFormatter($output);
            if (isset(self::$singleStreamHandler[$logPath])) {
                self::$singleStreamHandler[$logPath]->setFormatter(self::$singleFormatter[$logPath . "-" . $line]);
            }
        }
        self::getSingleLog($logPath);
        $realPath = self::$logSetting['logPath'] . self::$logSetting['logList']['general']['dir'] . $logPath . self::getSlice();
        if (!isset(self::$singleStreamHandlerPath[$logPath]) || self::$singleStreamHandlerPath[$logPath] != $realPath || !file_exists($realPath)) {
            self::$singleStreamHandler[$logPath] = new StreamHandler($realPath,
                self::$logSetting['logList']['general']['logLevel'], true, 0666);
            self::$singleStreamHandler[$logPath]->setFormatter(self::$singleFormatter[$logPath . "-" . $line]);
            //释放文件句柄
            if (!empty(self::$singleLog[$logPath]->getHandlers())) {
                self::$singleLog[$logPath]->popHandler()->close();
            }
            self::$singleLog[$logPath]->pushHandler(self::$singleStreamHandler[$logPath]);
            self::$singleStreamHandlerPath[$logPath] = $realPath;
        }
        self::writeLog($logPath, $logType, $logData);
    }

    private static function writeLog($logPath, $logType, $logData)
    {
        $log = self::getSingleLog($logPath);
        $message = Ip::getClientIp() . self::DEFAULE_LOG_SEPARATOR . array_shift($logData);
        if (PHP_SAPI == "fpm-fcgi" && isset($_SERVER['HTTP_USER_AGENT'])) {
            // 目前看，当前PHP核心业务中，curl以及queue::addMessage均会调用本log组件，所以直接在log组件中集成traceid
            $sTraceId = isset($_SERVER['HTTP_TRACEID']) ? $_SERVER['HTTP_TRACEID'] : "" ;
            $aTraceInfo = array(
                "trace" => $sTraceId,
            );
            array_push($logData, $aTraceInfo);
            array_push($logData, $_SERVER['REQUEST_URI'], $_SERVER['HTTP_USER_AGENT']);
        }
        $addFunction = 'add' . ucfirst($logType);
        $log->$addFunction($message, $logData);
    }

}
