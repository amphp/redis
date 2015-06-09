<?php

namespace Amp\Redis;

use Amp\NativeReactor;
use function Amp\run;

class DownTest extends \PHPUnit_Framework_TestCase {
    /**
     * @test
     * @expectedException \Amp\Redis\ConnectException
     */
    function ping () {
        (new NativeReactor())->run(function ($reactor) {
            $redis = new Client("tcp://127.0.0.1:25325", [], $reactor);
            yield $redis->ping();
        });
    }
}
