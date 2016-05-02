<?php

namespace Amp\Redis;

use Amp\Pause;
use Amp\PromiseStream;
use function Amp\driver;
use function Amp\reactor;
use function Amp\run;

class PubSubTest extends RedisTest {
    /**
     * @test
     */
    function basic() {
        reactor(driver())->run(function () {
            $subscriber = new SubscribeClient("tcp://127.0.0.1:25325");
            $subscription = $subscriber->subscribe("foo");

            $result = null;

            $subscription->watch(function ($response) use (&$result) {
                $result = $response;
            });

            $redis = new Client("tcp://127.0.0.1:25325");

            yield $redis->publish("foo", "bar");
            yield new Pause(500);

            $subscription->cancel();

            $this->assertEquals("bar", $result);
        });
    }

    /**
     * @test
     */
    function doubleCancel() {
        reactor(driver())->run(function () {
            $subscriber = new SubscribeClient("tcp://127.0.0.1:25325");
            $subscription = $subscriber->subscribe("foo");

            $subscription->cancel();
            $subscription->cancel();
        });
    }

    /**
     * @test
     */
    function multi() {
        reactor(driver())->run(function () {
            $subscriber = new SubscribeClient("tcp://127.0.0.1:25325");
            $subscription1 = $subscriber->subscribe("foo");
            $subscription2 = $subscriber->subscribe("foo");

            $result1 = $result2 = null;

            $subscription1->watch(function ($response) use (&$result1) {
                $result1 = $response;
            });

            $subscription2->watch(function ($response) use (&$result2) {
                $result2 = $response;
            });

            $redis = new Client("tcp://127.0.0.1:25325");

            yield $redis->publish("foo", "bar");
            yield new Pause(500);

            $this->assertEquals("bar", $result1);
            $this->assertEquals("bar", $result2);

            $subscription1->cancel();

            yield $redis->publish("foo", "xxx");
            yield new Pause(500);

            $this->assertEquals("bar", $result1);
            $this->assertEquals("xxx", $result2);

            $subscription2->cancel();
        });
    }

    /**
     * @test
     */
    function stream() {
        reactor(driver())->run(function () {
            $redis = new Client("tcp://127.0.0.1:25325");
            $subscriber = new SubscribeClient("tcp://127.0.0.1:25325");
            $subscription = $subscriber->subscribe("foo");

            $producer = \Amp\repeat(function () use ($redis) {
                $redis->publish("foo", "bar");
            }, 500);

            $lastResult = null;
            $consumed = 0;

            $subscriptionStream = new PromiseStream($subscription);

            while (yield $subscriptionStream->valid()) {
                $lastResult = $subscriptionStream->consume();
                $consumed++;

                if ($consumed === 3) {
                    $subscription->cancel();
                }
            }

            \Amp\cancel($producer);

            $this->assertSame(3, $consumed);
            $this->assertEquals("bar", $lastResult);
        });
    }
}
