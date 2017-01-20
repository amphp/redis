<?php

namespace Amp\Redis;

use function Amp\driver;
use function Amp\reactor;
use function Amp\run;
use AsyncInterop\Loop;

class PubSubTest extends RedisTest {
    /**
     * @test
     */
    function basic() {
        Loop::execute(\Amp\wrap(function () {
            $subscriber = new SubscribeClient("tcp://127.0.0.1:25325");
            $promise = $subscriber->subscribe("foo");

            $result = null;

            $promise->listen(function ($response) use (&$result) {
                $result = $response;
            });

            $redis = new Client("tcp://127.0.0.1:25325");

            yield $redis->publish("foo", "bar");
            yield $subscriber->unsubscribe("foo");

            $this->assertEquals("bar", $result);
        }));
    }
}
