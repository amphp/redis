<?php

namespace Amp\Redis;

use Amp\Loop;

class CloseTest extends RedisTest {
    /**
     * @test
     */
    function reconnect() {
        Loop::run(function () {
            $redis = new Client("tcp://127.0.0.1:25325");
            $this->assertEquals("PONG", (yield $redis->ping()));
            yield $redis->close();
            $this->assertEquals("PONG", (yield $redis->ping()));
        });
    }
}
