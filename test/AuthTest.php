<?php

namespace Amp\Redis;

use Amp\PHPUnit\AsyncTestCase;

class AuthTest extends AsyncTestCase
{
    public static function setUpBeforeClass()
    {
        print \shell_exec('redis-server --daemonize yes --port 25325 --timeout 3 --pidfile /tmp/amp-redis.pid --requirepass secret');
        \sleep(2);
    }

    public static function tearDownAfterClass()
    {
        $pid = @\file_get_contents('/tmp/amp-redis.pid');
        @\unlink('/tmp/amp-redis.pid');

        if (!empty($pid)) {
            print \shell_exec("kill $pid");
            \sleep(2);
        }
    }

    public function testEcho(): \Generator
    {
        $redis = new Redis(new RemoteExecutor('tcp://127.0.0.1:25325?password=secret'));
        $this->assertEquals('PONG', yield $redis->echo('PONG'));
        $redis->quit();
    }
}
