<?php

namespace Amp\Redis;

use Amp\NativeReactor;
use function Amp\run;

class CloseTest extends RedisTest {
    /**
     * @test
     */
    function reconnect () {
        (new NativeReactor())->run(function ($reactor) {
            $redis = new Client("tcp://127.0.0.1:25325", null, $reactor);
            $this->assertEquals("PONG", (yield $redis->ping()));
            yield $redis->close();
            $this->assertEquals("PONG", (yield $redis->ping()));
        });
    }
}
