<?php

chdir(__DIR__);
error_reporting(E_ALL);

use Amp\Redis\RespParser;

const DEBUG = true;

require "../vendor/autoload.php";

Amp\run(function () {
    $parser = new RespParser(function ($data) {
        // ignore for now
    });

    $time = PHP_INT_MAX;

    for ($x = 0; $x < 10; $x++) {
        $start = microtime(1);

        for ($i = 0; $i < 1000000; $i++) {
            $parser->append("*2\r\n$5\r\nHello\r\n:123456789\r\n");
        }

        $time = min($time, microtime(1) - $start);
    }

    var_dump($time);
});
