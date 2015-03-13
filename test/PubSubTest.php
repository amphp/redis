<?php

namespace Amp\Redis;

use Amp\NativeReactor;
use function Amp\run;

class PubSubTest extends \PHPUnit_Framework_TestCase {
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

    /**
     * @test
     */
    function basic () {
        (new NativeReactor())->run(function ($reactor) {
            $subscriber = new SubscribeClient("tcp://127.0.0.1:25325", null, $reactor);
            $promise = $subscriber->subscribe("foo");

            $result = null;

            $promise->watch(function($response) use (&$result) {
                $result = $response;
            });

            $redis = new Client("tcp://127.0.0.1:25325", null, $reactor);

            yield $redis->publish("foo", "bar");
            yield $subscriber->unsubscribe("foo");

            $this->assertEquals("bar", $result);

            $subscriber->close();
            $redis->close();
        });
    }
}
