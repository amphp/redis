<?php

namespace Amp\Redis;

use Amp\NativeReactor;
use function Amp\run;

class BasicTest extends RedisTest {
    /**
     * @test
     */
    function connect () {
        (new NativeReactor())->run(function ($reactor) {
            $redis = new Client("tcp://127.0.0.1:25325", null, $reactor);
            $this->assertEquals("PONG", (yield $redis->ping()));
            $redis->close();
        });
    }

    /**
     * @test
     */
    function multiCommand () {
        (new NativeReactor())->run(function ($reactor) {
            $redis = new Client("tcp://127.0.0.1:25325", null, $reactor);
            $redis->echo("1");
            $this->assertEquals("2", (yield $redis->echo("2")));
            $redis->close();
        });
    }

    /**
     * @test
     * @medium
     */
    function timeout () {
        (new NativeReactor())->run(function ($reactor) {
            $redis = new Client("tcp://127.0.0.1:25325", null, $reactor);
            yield $redis->echo("1");

            yield "pause" => 8000;

            $this->assertEquals("2", (yield $redis->echo("2")));
            $redis->close();
        });
    }
}
