<?php

chdir(__DIR__);
error_reporting(E_ALL);

class BenchCase {
    private $callable;

    public function __construct (callable $callable) {
        $this->callable = $callable;
    }

    public function bench () {
        $time = PHP_INT_MAX;

        for ($x = 0; $x < 10; $x++) {
            $start = microtime(1);

            for ($i = 0; $i < 1000000; $i++) {
                ($this->callable)(null);
            }

            $time = min($time, microtime(1) - $start);
        }

        return $time;
    }

    protected function onData ($data) {
        // empty on purpose!
    }
}

class AnonymousFunctionCase extends BenchCase {
    public function __construct () {
        parent::__construct(function ($data) {
            $this->onData($data);
        });
    }
}

class ReflectionCase extends BenchCase {
    public function __construct () {
        $reflection = new ReflectionClass(self::class);
        $closure = $reflection->getMethod("onData")->getClosure($this);

        parent::__construct($closure);
    }
}

$test = new AnonymousFunctionCase;
printf("anon function: %f\n", $test->bench());

$test = new ReflectionCase;
printf("reflection:    %f\n", $test->bench());
