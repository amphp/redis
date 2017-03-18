<?php

namespace Amp\Redis;

use Amp\Loop;

class DownTest extends \PHPUnit_Framework_TestCase {
    /**
     * @test
     * @expectedException \Amp\Redis\ConnectException
     */
    function ping() {
        Loop::run(function () {
            $redis = new Client("tcp://127.0.0.1:25325");
            yield $redis->ping();
        });
    }
}
