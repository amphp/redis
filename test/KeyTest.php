<?php

namespace Amp\Redis;

use Amp\NativeReactor;
use function Amp\run;

class KeyTest extends RedisTest {
    /**
     * @test
     */
    function keys () {
        (new NativeReactor())->run(function ($reactor) {
            $redis = new Client("tcp://127.0.0.1:25325", null, $reactor);
            $this->assertEquals([], (yield $redis->keys("*")));
            $redis->set("foo", 42);
            $this->assertEquals(["foo"], (yield $redis->keys("*")));
            $redis->close();
        });
    }

    /**
     * @test
     */
    function exists () {
        (new NativeReactor())->run(function ($reactor) {
            $redis = new Client("tcp://127.0.0.1:25325", null, $reactor);
            $this->assertTrue((yield $redis->exists("foo")));
            $this->assertFalse((yield $redis->exists("bar")));
            $redis->close();
        });
    }

    /**
     * @test
     */
    function del () {
        (new NativeReactor())->run(function ($reactor) {
            $redis = new Client("tcp://127.0.0.1:25325", null, $reactor);
            $this->assertTrue((yield $redis->exists("foo")));
            $redis->del("foo");
            $this->assertFalse((yield $redis->exists("foo")));
            $redis->close();
        });
    }
}

