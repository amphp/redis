<?php

namespace Amp\Redis;

use Amp\Pause;
use function Amp\driver;
use function Amp\reactor;
use function Amp\run;
use AsyncInterop\Loop;

class BasicTest extends RedisTest {
    /**
     * @test
     */
    function connect() {
        Loop::execute(\Amp\wrap(function () {
            $redis = new Client("tcp://127.0.0.1:25325");
            $this->assertEquals("PONG", (yield $redis->ping()));
        }));
    }

    /**
     * @test
     */
    function longPayload() {
        Loop::execute(\Amp\wrap(function () {
            $redis = new Client("tcp://127.0.0.1:25325");
            $payload = str_repeat("a", 6000000);
            yield $redis->set("foobar", $payload);
            $this->assertEquals($payload, (yield $redis->get("foobar")));
        }));
    }

    /**
     * @test
     * @expectedException \InvalidArgumentException
     */
    function acceptsOnlyScalars() {
        Loop::execute(\Amp\wrap(function () {
            $redis = new Client("tcp://127.0.0.1:25325");
            $redis->set("foobar", ["abc"]);
        }));
    }

    /**
     * @test
     */
    function multiCommand() {
        Loop::execute(\Amp\wrap(function () {
            $redis = new Client("tcp://127.0.0.1:25325");
            $redis->echo("1");
            $this->assertEquals("2", (yield $redis->echo("2")));
        }));
    }

    /**
     * @test
     * @medium
     */
    function timeout() {
        Loop::execute(\Amp\wrap(function () {
            $redis = new Client("tcp://127.0.0.1:25325");
            yield $redis->echo("1");
            yield new Pause(8000);
            $this->assertEquals("2", (yield $redis->echo("2")));
        }));
    }
}
