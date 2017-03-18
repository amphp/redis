<?php

namespace Amp\Redis;

use Amp\Loop;

class KeyTest extends RedisTest {
    /**
     * @test
     */
    function keys() {
        Loop::run(function () {
            $redis = new Client("tcp://127.0.0.1:25325");
            $this->assertEquals([], (yield $redis->keys("*")));
            $redis->set("foo", 42);
            $this->assertEquals(["foo"], (yield $redis->keys("*")));
        });
    }

    /**
     * @test
     */
    function exists() {
        Loop::run(function () {
            $redis = new Client("tcp://127.0.0.1:25325");
            $this->assertTrue((yield $redis->exists("foo")));
            $this->assertFalse((yield $redis->exists("bar")));
        });
    }

    /**
     * @test
     */
    function del() {
        Loop::run(function () {
            $redis = new Client("tcp://127.0.0.1:25325");
            $this->assertTrue((yield $redis->exists("foo")));
            $redis->del("foo");
            $this->assertFalse((yield $redis->exists("foo")));
        });
    }
}

