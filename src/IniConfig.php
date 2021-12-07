<?php

namespace Hhz\DB;

class IniConfig
{
    public $database;

    public function __construct()
    {
        $config = new Yaf_Config_ini(APPLICATION_PATH.'/conf/application.ini');
        $config = $config->toArray();
        $this->database = $config['database'];
    }

    public function getDatabaseConfig()
    {
        return $this->database;
    }
}