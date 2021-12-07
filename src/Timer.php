<?php

namespace Hhz\DB;


class Timer
{
    const DEFAULE_SIGN = 'default';
    const UNIT_MS = 'ms';
    const UNIT_S = 's';
    private static $startTimes = [];

    public static function start($sign = self::DEFAULE_SIGN)
    {
        self::$startTimes[$sign] = microtime(true);
    }

    public static function stop($sign = self::DEFAULE_SIGN, $unit = self::UNIT_S)
    {
        $time = round((microtime(true) - self::$startTimes[$sign]) * 1000);
        if ($unit == self::UNIT_MS) {
            return $time;
        } else {
            return floor($time / 1000 * 100.0) / 100.0;
        }
    }

    /**
     * 获取一周的开始结束时间
     * @param string $time
     * @param int $first
     * @return array
     */
    public static function getWeekStartAndEnd($time = '', $first = 1)
    {
        if (!$time) {
            $time = time();
        }
        $defaultDate = date("Y-m-d", $time);
        // $first =1 表示每周星期一为开始日期 0表示每周日为开始日期 获取当前周的第几天 周日是 0 周一到周六是 1 - 6
        $w = date('w', strtotime($defaultDate));
        // 获取本周开始日期，如果$w是0，则表示周日，减去 6 天
        $startTimestamp = strtotime("$defaultDate -" . ($w ? $w - $first : 6) . ' days');
        $week_start = date('Y-m-d H:i:s', $startTimestamp);
        // 本周结束日期
        $endTimestamp = strtotime("$week_start +6 days");
        $week_end = date('Y-m-d H:i:s', $endTimestamp);
        return ['start_time' => $week_start, 'end_time' => $week_end, 'start_timestamp' => $startTimestamp, 'end_timestamp' => $endTimestamp];
    }
}