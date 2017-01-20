<?php

namespace Amp\Redis;

use AsyncInterop\Loop;

class DownTest extends \PHPUnit_Framework_TestCase {
    /**
     * @test
     * @expectedException \Amp\Redis\ConnectException
     */
    function ping() {
        Loop::execute(\Amp\wrap(function () {
            $redis = new Client("tcp://127.0.0.1:25325");
            yield $redis->ping();
        }));
    }
}
