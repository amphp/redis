<?php

namespace Amp\Redis;

use Amp\Iterator;
use Amp\Loop;

class PubSubTest extends RedisTest {
    /**
     * @test
     */
    public function basic() {
        Loop::run(function () {
            $subscriber = new SubscribeClient("tcp://127.0.0.1:25325");

            /** @var Iterator $iterator */
            $iterator = yield $subscriber->subscribe("foo");

            $result = null;

            $iterator->advance()->onResolve(function () use (&$result, $iterator) {
                $result = $iterator->getCurrent();
            });

            $redis = new Client("tcp://127.0.0.1:25325");

            yield $redis->publish("foo", "bar");
            yield $subscriber->unsubscribe("foo");

            $this->assertEquals("bar", $result);
        });
    }
}
