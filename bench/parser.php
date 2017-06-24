<?php

chdir(__DIR__);
error_reporting(E_ALL);

use Amp\Redis\RespParser;

const DEBUG = true;

require "../vendor/autoload.php";

class BenchCase {
    private $parser;

    public function __construct(RespParser $parser) {
        $this->parser = $parser;
    }

    public function bench() {
        $time = PHP_INT_MAX;

        for ($x = 0; $x < 10; $x++) {
            $start = microtime(1);

            for ($i = 0; $i < 1000000; $i++) {
                $this->parser->append("$5\r\nHello\r\n:123456789\r\n$5\r\nHello\r\n:123456789\r\n$5\r\nHello\r\n:123456789\r\n$5\r\nHello\r\n:123456789\r\n");
            }

            $time = min($time, microtime(1) - $start);
        }

        return $time;
    }

    protected function onData($data) {
        // empty on purpose!
    }
}

class AnonymousFunctionCase extends BenchCase {
    public function __construct() {
        parent::__construct(new RespParser(function ($data) {
            $this->onData($data);
        }));
    }
}

class ReflectionCase extends BenchCase {
    public function __construct() {
        $reflection = new ReflectionClass(self::class);
        $closure = $reflection->getMethod("onData")->getClosure($this);

        parent::__construct(new RespParser($closure));
    }
}

Amp\Loop::run(function () {
    $test = new AnonymousFunctionCase;
    printf("anon function: %f\n", $test->bench());

    $test = new ReflectionCase;
    printf("reflection:    %f\n", $test->bench());
});
