<?php

namespace Amp\Redis;

use function Amp\driver;
use function Amp\reactor;
use function Amp\run;

class AuthTest extends \PHPUnit_Framework_TestCase {
    static function setUpBeforeClass() {
        print `redis-server --daemonize yes --port 25325 --timeout 3 --pidfile /tmp/amp-redis.pid --requirepass secret`;
        sleep(2);
    }

    static function tearDownAfterClass() {
        $pid = @file_get_contents("/tmp/amp-redis.pid");
        @unlink("/tmp/amp-redis.pid");

        if (!empty($pid)) {
            print `kill $pid`;
            sleep(2);
        }
    }

    /**
     * @test
     */
    function ping() {
        reactor(driver())->run(function () {
            $redis = new Client("tcp://127.0.0.1:25325?password=secret");
            $this->assertEquals("PONG", (yield $redis->ping()));
            $redis->close();
        });
    }
}
