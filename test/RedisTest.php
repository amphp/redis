<?php

namespace Amp\Redis;

abstract class RedisTest extends \PHPUnit_Framework_TestCase {
    static function setUpBeforeClass () {
        print `redis-server --daemonize yes --port 25325 --timeout 3 --pidfile /tmp/amp-redis.pid`;
        sleep(2);
    }

    static function tearDownAfterClass () {
        $pid = @file_get_contents("/tmp/amp-redis.pid");
        @unlink("/tmp/amp-redis.pid");

        if (!empty($pid)) {
            print `kill $pid`;
            sleep(2);
        }
    }
}