<?php

namespace Amp\Redis;

use Amp\Pipeline\ConcurrentIterableIterator;
use Amp\Pipeline\ConcurrentIterator;
use Revolt\EventLoop;
use function Amp\async;
use function Amp\delay;

class PubSubTest extends IntegrationTest
{
    protected function setUp(): void
    {
        parent::setUp();
        $this->setTimeout(1);
    }

    private function getNextValue(ConcurrentIterator $concurrentIterator): string
    {
        if (!$concurrentIterator->continue()) {
            self::fail('Expected value from redis subscription');
        }

        return $concurrentIterator->getValue();
    }

    public function testBasic(): void
    {
        $subscriber = new Subscriber(RedisConfig::fromUri($this->getUri()));

        $subscription = $subscriber->subscribe('foo');
        $iterator = new ConcurrentIterableIterator($subscription);

        // Use async() to not block, because we publish in the same coroutine
        $promise = async(fn () => $this->getNextValue($iterator));

        delay(0.1); // Enter event loop so subscriber has time to connect.

        $this->redis->publish('foo', 'bar');

        delay(0.1); // Enter event loop so subscription has time to receive publish.

        $subscription->unsubscribe();

        delay(0.1);

        $this->assertEquals('bar', $promise->await());
    }

    public function testDoubleCancel(): void
    {
        $subscriber = new Subscriber(RedisConfig::fromUri($this->getUri()));

        $subscription = $subscriber->subscribe('foo');
        $subscription->unsubscribe();
        $subscription->unsubscribe();

        delay(0.1); // Ensure cancel request has completed.

        $this->assertTrue(true);
    }

    public function testMulti(): void
    {
        $subscriber = new Subscriber(RedisConfig::fromUri($this->getUri()));

        $subscription1 = $subscriber->subscribe('foo');
        $iterator1 = new ConcurrentIterableIterator($subscription1);
        $subscription2 = $subscriber->subscribe('foo');
        $iterator2 = new ConcurrentIterableIterator($subscription2);

        delay(0.1); // Enter event loop so subscriber has time to connect.

        $this->redis->publish('foo', 'bar');

        $this->assertEquals('bar', $this->getNextValue($iterator1));
        $this->assertEquals('bar', $this->getNextValue($iterator2));

        $subscription1->unsubscribe();

        $this->redis->publish('foo', 'xxx');

        $this->assertEquals('xxx', $this->getNextValue($iterator2));

        $subscription2->unsubscribe();

        delay(0.1); // Ensure cancel request has completed.
    }

    public function testStream(): void
    {
        $subscriber = new Subscriber(RedisConfig::fromUri($this->getUri()));

        $subscription = $subscriber->subscribe('foo');
        $iterator = new ConcurrentIterableIterator($subscription);

        $producer = EventLoop::repeat(0.1, function (): void {
            $this->redis->publish('foo', 'bar');
        });

        $consumed = 0;

        while ($lastResult = $this->getNextValue($iterator)) {
            $consumed++;
            if ($consumed === 3) {
                $subscription->unsubscribe();
                break;
            }
        }

        EventLoop::cancel($producer);

        $this->assertSame(3, $consumed);
        $this->assertEquals('bar', $lastResult);

        delay(0.1); // Ensure cancel request has completed.
    }
}
