<?php

namespace Hhz\DB;

use Medoo\Medoo;
use PDO;
use Symfony\Component\Cache\Adapter\FilesystemAdapter;


/**
 *
 * @author 徐冠群
 *
 */
class MedooScrew extends Medoo
{
    const SQL_TRACK = true;

    private static $fileCahce = NULL;
    private static $tableColumns = [];
    private static $tableName = NULL;
    private static $tableAlias = NULL;
    private static $column_type_int = [
        "tinyint"   => true,
        "smallint"  => true,
        "mediumint" => true,
        "int"       => true,
        "bigint"    => true,
    ];


    private static function FileCahce()
    {
        if (is_null(self::$fileCahce)) {
            self::$fileCahce = new FilesystemAdapter('dbColumnsType', 0, Log::DEFAULT_LOG_PATH_PREFIX . "medoo_screw_cache");
        }
        return self::$fileCahce;
    }

    private function getTableColumnsFromCache($dns, $tableName)
    {
        $cache = self::FileCahce();
        $cacheKey = md5($dns . $tableName);
        $TableCache = $cache->getItem($cacheKey);
        if ($TableCache->isHit()) {
            return json_decode($TableCache->get(), true);
        } else {
            return false;
        }
    }

    private function setTableColumnsFromCache($dns, $tableName, $tableColumns)
    {
        $cache = self::FileCahce();
        $cacheKey = md5($dns . $tableName);
        $TableCache = $cache->getItem($cacheKey);
        $TableCache->set(json_encode($tableColumns));
        $cache->save($TableCache);
    }

    private function delTableColumnsFromCache($dns, $tableName)
    {
        $cache = self::FileCahce();
        $cacheKey = md5($dns . $tableName);
        $cache->deleteItem($cacheKey);
    }

    public function getTableColumns($tableName = NULL)
    {
        if (!is_null($tableName)) {
            preg_match('/(?<table>[a-zA-Z0-9_]+)\s*\((?<alias>[a-zA-Z0-9_]+)\)/i', $tableName, $table_match);
            if ($table_match) {
                $tableName = $table_match['table'];
                static::$tableName = $table_match['table'];
                static::$tableAlias = $table_match['alias'];
            } else {
                static::$tableName = $tableName;
            }
            if (!isset(static::$tableColumns[$this->dsn][$tableName])) {
                $shmColumnsType = $this->getTableColumnsFromCache($this->dsn, $tableName);
                if ($shmColumnsType) {
                    static::$tableColumns[$this->dsn][$tableName] = $shmColumnsType;
                } else {
                    $dbName = $this->getDbNameFromDsn($this->dsn);
                    $sth = $this->pdo->prepare('SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns WHERE  table_schema = :table_schema AND table_name=:table_name');
                    $sth->execute(array(
                        ':table_schema' => $dbName,
                        ':table_name'   => $tableName
                    ));
                    $columns = $sth->fetchAll(PDO::FETCH_ASSOC);
                    $columnsType = [];
                    foreach ($columns as $column) {
                        if (isset(self::$column_type_int[$column['DATA_TYPE']])) {
                            $columnsType[$column['COLUMN_NAME']] = PDO::PARAM_INT;
                        } else {
                            $columnsType[$column['COLUMN_NAME']] = PDO::PARAM_STR;
                        }
                    }
                    static::$tableColumns[$this->dsn][$tableName] = $columnsType;
                    $this->setTableColumnsFromCache($this->dsn, $tableName, $columnsType);
                }
            }
        }
    }

    protected function getDbNameFromDsn($dsn)
    {
        $paramsStr = explode(";", $dsn);
        $params = [];
        foreach ($paramsStr as $paramStr) {
            $paramArr = explode("=", $paramStr);
            $params[$paramArr[0]] = isset($paramArr[1]) ? $paramArr[1] : "";
        }
        if (isset($params['mysql:dbname'])) {
            return $params['mysql:dbname'];
        } else {
            return "";
        }
    }

    protected function typeMap($value, $type, $column = null)
    {
        $typeMap = parent::typeMap($value, $type);
        if (!is_null($column) && !is_null(static::$tableName)) {
            if (strpos($column, ".") === false) {
                $column = trim($column, '"');
            } else {
                $columnMatch = explode(".", $column);
                if (static::$tableAlias == $columnMatch[0]) {
                    $column = trim($columnMatch[1], '"');
                } else {
                    Log::db_cache_info($this->dsn . '#' . static::$tableName,
                        [
                            $value,
                            $type,
                            $column
                        ]);
                    return $typeMap;
                }
            }
            if (isset(static::$tableColumns[$this->dsn][static::$tableName][$column])) {
                $columnType = static::$tableColumns[$this->dsn][static::$tableName][$column];
                if ($columnType == PDO::PARAM_INT && $typeMap[1] == PDO::PARAM_STR) {
                    return [
                        (int)$typeMap[0],
                        PDO::PARAM_INT
                    ];
                } elseif ($columnType == PDO::PARAM_STR && $typeMap[1] == PDO::PARAM_INT) {
                    return [
                        (string)$typeMap[0],
                        PDO::PARAM_STR
                    ];
                } else {
                    return $typeMap;
                }
            } else {
                Log::db_cache_error($this->dsn . '#' . static::$tableName,
                    [
                        $value,
                        $type,
                        $column
                    ]);
                $this->delTableColumnsFromCache($this->dsn, static::$tableName);
                return $typeMap;
            }
        } else {
            return $typeMap;
        }
    }

    protected function dataImplode($data, &$map, $conjunctor)
    {
        $stack = [];

        foreach ($data as $key => $value) {
            $type = gettype($value);

            if (
                $type === 'array' &&
                preg_match("/^(AND|OR)(\s+#.*)?$/", $key, $relation_match)
            ) {
                $relationship = $relation_match[1];

                $stack[] = $value !== array_keys(array_keys($value)) ?
                    '(' . $this->dataImplode($value, $map, ' ' . $relationship) . ')' :
                    '(' . $this->innerConjunct($value, $map, ' ' . $relationship, $conjunctor) . ')';

                continue;
            }

            $map_key = $this->mapKey();

            if (
                is_int($key) &&
                preg_match('/([a-zA-Z0-9_\.]+)\[(?<operator>\>\=?|\<\=?|\!?\=)\]([a-zA-Z0-9_\.]+)/i', $value, $match)
            ) {
                $stack[] = $this->columnQuote($match[1]) . ' ' . $match['operator'] . ' ' . $this->columnQuote($match[3]);
            } else {
                preg_match('/([a-zA-Z0-9_\.]+)(\[(?<operator>\>\=?|\<\=?|\!|\<\>|\>\<|\!?~|REGEXP)\])?/i', $key, $match);
                $column = $this->columnQuote($match[1]);

                if (isset($match['operator'])) {
                    $operator = $match['operator'];

                    if (in_array($operator, [
                        '>',
                        '>=',
                        '<',
                        '<='
                    ])) {
                        $condition = $column . ' ' . $operator . ' ';

                        if (is_numeric($value)) {
                            $condition .= $map_key;
                            $map[$map_key] = [
                                $value,
                                PDO::PARAM_INT
                            ];
                        } elseif ($raw = $this->buildRaw($value, $map)) {
                            $condition .= $raw;
                        } else {
                            $condition .= $map_key;
                            $map[$map_key] = [
                                $value,
                                PDO::PARAM_STR
                            ];
                        }

                        $stack[] = $condition;
                    } elseif ($operator === '!') {
                        switch ($type) {
                            case 'NULL':
                                $stack[] = $column . ' IS NOT NULL';
                                break;

                            case 'array':
                                $placeholders = [];

                                foreach ($value as $index => $item) {
                                    $placeholders[] = $map_key . $index . '_i';
                                    $map[$map_key . $index . '_i'] = $this->typeMap($item, gettype($item), $column);
                                }

                                $stack[] = $column . ' NOT IN (' . implode(', ', $placeholders) . ')';
                                break;

                            case 'object':
                                if ($raw = $this->buildRaw($value, $map)) {
                                    $stack[] = $column . ' != ' . $raw;
                                }
                                break;

                            case 'integer':
                            case 'double':
                            case 'boolean':
                            case 'string':
                                $stack[] = $column . ' != ' . $map_key;
                                $map[$map_key] = $this->typeMap($value, $type, $column);
                                break;
                        }
                    } elseif ($operator === '~' || $operator === '!~') {
                        if ($type !== 'array') {
                            $value = [$value];
                        }

                        $connector = ' OR ';
                        $data = array_values($value);

                        if (is_array($data[0])) {
                            if (isset($value['AND']) || isset($value['OR'])) {
                                $connector = ' ' . array_keys($value)[0] . ' ';
                                $value = $data[0];
                            }
                        }

                        $like_clauses = [];

                        foreach ($value as $index => $item) {
                            $item = strval($item);

                            if (!preg_match('/(\[.+\]|_|%.+|.+%)/', $item)) {
                                $item = '%' . $item . '%';
                            }

                            $like_clauses[] = $column . ($operator === '!~' ? ' NOT' : '') . ' LIKE ' . $map_key . 'L' . $index;
                            $map[$map_key . 'L' . $index] = [
                                $item,
                                PDO::PARAM_STR
                            ];
                        }

                        $stack[] = '(' . implode($connector, $like_clauses) . ')';
                    } elseif ($operator === '<>' || $operator === '><') {
                        if ($type === 'array') {
                            if ($operator === '><') {
                                $column .= ' NOT';
                            }

                            $stack[] = '(' . $column . ' BETWEEN ' . $map_key . 'a AND ' . $map_key . 'b)';

                            $data_type = (is_numeric($value[0]) && is_numeric($value[1])) ? PDO::PARAM_INT : PDO::PARAM_STR;

                            $map[$map_key . 'a'] = [
                                $value[0],
                                $data_type
                            ];
                            $map[$map_key . 'b'] = [
                                $value[1],
                                $data_type
                            ];
                        }
                    } elseif ($operator === 'REGEXP') {
                        $stack[] = $column . ' REGEXP ' . $map_key;
                        $map[$map_key] = [
                            $value,
                            PDO::PARAM_STR
                        ];
                    }
                } else {
                    switch ($type) {
                        case 'NULL':
                            $stack[] = $column . ' IS NULL';
                            break;

                        case 'array':
                            $placeholders = [];

                            foreach ($value as $index => $item) {
                                $placeholders[] = $map_key . $index . '_i';
                                $map[$map_key . $index . '_i'] = $this->typeMap($item, gettype($item), $column);
                            }

                            $stack[] = $column . ' IN (' . implode(', ', $placeholders) . ')';
                            break;

                        case 'object':
                            if ($raw = $this->buildRaw($value, $map)) {
                                $stack[] = $column . ' = ' . $raw;
                            }
                            break;

                        case 'integer':
                        case 'double':
                        case 'boolean':
                        case 'string':
                            $stack[] = $column . ' = ' . $map_key;
                            $map[$map_key] = $this->typeMap($value, $type, $column);
                            break;
                    }
                }
            }
        }

        return implode($conjunctor . ' ', $stack);
    }

    public function query($query, $map = [])
    {
        if (self::SQL_TRACK && is_string($query)) {
            $subject = isset($_SERVER['REQUEST_URI']) ? $_SERVER['REQUEST_URI'] : $_SERVER['SCRIPT_FILENAME'];
            $trackInfo = str_replace('*', '', $subject);
            $query .= " /* {$trackInfo} */";
        }
        return parent::query($query, $map);
    }

    public function exec($query, $map = [])
    {
        if (self::SQL_TRACK && is_string($query)) {
            $subject = isset($_SERVER['REQUEST_URI']) ? $_SERVER['REQUEST_URI'] : $_SERVER['SCRIPT_FILENAME'];
            $trackInfo = str_replace('*', '', $subject);
            $query .= " /* {$trackInfo} */";
        }
        return parent::exec($query, $map);
    }
}

