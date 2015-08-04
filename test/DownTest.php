<?php

namespace Amp\Redis;

use function Amp\driver;
use Amp\NativeReactor;
use function Amp\reactor;
use function Amp\run;

class DownTest extends \PHPUnit_Framework_TestCase {
    /**
     * @test
     * @expectedException \Amp\Redis\ConnectException
     */
    function ping () {
        reactor(driver())->run(function () {
            $redis = new Client("tcp://127.0.0.1:25325", []);
            yield $redis->ping();
        });
    }
}
