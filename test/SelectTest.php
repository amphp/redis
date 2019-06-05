<?php

namespace Amp\Redis;

use Amp\Loop;

class SelectTest extends RedisTest
{
    public static function setUpBeforeClass()
    {
        print `redis-server --daemonize yes --port 25325 --timeout 3 --pidfile /tmp/amp-redis.pid --requirepass secret`;
        \sleep(2);
    }

    /**
     * @test
     */
    public function connect()
    {
        Loop::run(function () {
            $redis = new Client("tcp://127.0.0.1:25325?database=1&password=secret");
            $this->assertEquals("PONG", (yield $redis->ping()));
        });
    }

    /**
     * @test
     */
    public function select()
    {
        Loop::run(function () {
            $redis1 = new Client("tcp://127.0.0.1:25325?database=1&password=secret");
            $payload = "bar";
            yield $redis1->set("foobar", $payload);
            $this->assertEquals($payload, (yield $redis1->get("foobar")));

            $redis0 = new Client("tcp://127.0.0.1:25325?password=secret");
            $this->assertNotEquals($payload, (yield $redis0->get("foobar")));
        });
    }
}
