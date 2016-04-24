<?php

namespace Amp\Redis;

use Amp\Pause;
use function Amp\driver;
use function Amp\reactor;
use function Amp\run;

class BasicTest extends RedisTest {
    /**
     * @test
     */
    function connect() {
        reactor(driver())->run(function () {
            $redis = new Client("tcp://127.0.0.1:25325", []);
            $this->assertEquals("PONG", (yield $redis->ping()));
            $redis->close();
        });
    }

    /**
     * @test
     */
    function longPayload() {
        reactor(driver())->run(function () {
            $redis = new Client("tcp://127.0.0.1:25325", []);
            $payload = str_repeat("a", 6000000);
            yield $redis->set("foobar", $payload);
            $this->assertEquals($payload, (yield $redis->get("foobar")));
            $redis->close();
        });
    }

    /**
     * @test
     */
    function multiCommand() {
        reactor(driver())->run(function () {
            $redis = new Client("tcp://127.0.0.1:25325", []);
            $redis->echo("1");
            $this->assertEquals("2", (yield $redis->echo("2")));
            $redis->close();
        });
    }

    /**
     * @test
     * @medium
     */
    function timeout() {
        reactor(driver())->run(function () {
            $redis = new Client("tcp://127.0.0.1:25325", []);
            yield $redis->echo("1");

            yield new Pause(8000);

            $this->assertEquals("2", (yield $redis->echo("2")));
            $redis->close();
        });
    }
}
