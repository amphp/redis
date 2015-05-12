<?php

namespace Amp\Redis;

use Amp\NativeReactor;
use function Amp\run;

class PubSubTest extends RedisTest {
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
