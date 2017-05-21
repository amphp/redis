<?php

namespace Amp\Redis;

use Amp\Delayed;
use Amp\Loop;

class BasicTest extends RedisTest {
    /**
     * @test
     */
    public function connect() {
        Loop::run(function () {
            $redis = new Client("tcp://127.0.0.1:25325");
            $this->assertEquals("PONG", (yield $redis->ping()));
        });
    }

    /**
     * @test
     */
    public function longPayload() {
        Loop::run(function () {
            $redis = new Client("tcp://127.0.0.1:25325");
            $payload = str_repeat("a", 6000000);
            yield $redis->set("foobar", $payload);
            $this->assertEquals($payload, (yield $redis->get("foobar")));
        });
    }

    /**
     * @test
     * @expectedException \InvalidArgumentException
     */
    public function acceptsOnlyScalars() {
        Loop::run(function () {
            $redis = new Client("tcp://127.0.0.1:25325");
            $redis->set("foobar", ["abc"]);
        });
    }

    /**
     * @test
     */
    public function multiCommand() {
        Loop::run(function () {
            $redis = new Client("tcp://127.0.0.1:25325");
            $redis->echo("1");
            $this->assertEquals("2", (yield $redis->echo("2")));
        });
    }

    /**
     * @test
     * @medium
     */
    public function timeout() {
        Loop::run(function () {
            $redis = new Client("tcp://127.0.0.1:25325");
            yield $redis->echo("1");
            yield new Delayed(8000);
            $this->assertEquals("2", (yield $redis->echo("2")));
        });
    }
}
