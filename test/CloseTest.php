<?php

namespace Amp\Redis;

use function Amp\driver;
use function Amp\reactor;
use function Amp\run;
use AsyncInterop\Loop;

class CloseTest extends RedisTest {
    /**
     * @test
     */
    function reconnect() {
        Loop::execute(\Amp\wrap(function () {
            $redis = new Client("tcp://127.0.0.1:25325");
            $this->assertEquals("PONG", (yield $redis->ping()));
            yield $redis->close();
            $this->assertEquals("PONG", (yield $redis->ping()));
        }));
    }
}
