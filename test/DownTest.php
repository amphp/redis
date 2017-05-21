<?php

namespace Amp\Redis;

use Amp\Loop;
use PHPUnit\Framework\TestCase;

class DownTest extends TestCase {
    /**
     * @test
     * @expectedException \Amp\Redis\ConnectException
     */
    public function ping() {
        Loop::run(function () {
            $redis = new Client("tcp://127.0.0.1:25325");
            yield $redis->ping();
        });
    }
}
