<?php

namespace Amp\Redis;

use function Amp\driver;
use function Amp\reactor;
use function Amp\run;

class PubSubTest extends RedisTest {
    /**
     * @test
     */
    function basic () {
        reactor(driver())->run(function () {
            $subscriber = new SubscribeClient("tcp://127.0.0.1:25325", []);
            $promise = $subscriber->subscribe("foo");

            $result = null;

            $promise->watch(function ($response) use (&$result) {
                $result = $response;
            });

            $redis = new Client("tcp://127.0.0.1:25325", []);

            yield $redis->publish("foo", "bar");
            yield $subscriber->unsubscribe("foo");

            $this->assertEquals("bar", $result);

            $subscriber->close();
            $redis->close();
        });
    }
}
