<?php declare(strict_types=1);

namespace Amp\Redis;

use Amp\Pipeline\ConcurrentIterator;
use Amp\Pipeline\Pipeline;
use Revolt\EventLoop;
use function Amp\async;
use function Amp\delay;

class PubSubTest extends IntegrationTest
{
    private RedisSubscriber $subscriber;

    protected function setUp(): void
    {
        parent::setUp();
        $this->setTimeout(1);

        $this->subscriber = new RedisSubscriber(createRedisConnector($this->getUri()));
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
        $subscription = $this->subscriber->subscribe('foo');
        $iterator = Pipeline::fromIterable($subscription)->getIterator();

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
        $subscription = $this->subscriber->subscribe('foo');
        $subscription->unsubscribe();
        $subscription->unsubscribe();

        delay(0.1); // Ensure cancel request has completed.

        $this->assertTrue(true);
    }

    public function testMulti(): void
    {
        $subscription1 = $this->subscriber->subscribe('foo');
        $iterator1 = Pipeline::fromIterable($subscription1)->getIterator();
        $subscription2 = $this->subscriber->subscribe('foo');
        $iterator2 = Pipeline::fromIterable($subscription2)->getIterator();

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
        $subscription = $this->subscriber->subscribe('foo');
        $iterator = Pipeline::fromIterable($subscription)->getIterator();

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

    public function testIteratorReferenceOnlyDoesNotUnsubscribe(): void
    {
        $iterator = $this->subscriber->subscribe('foo')->getIterator();

        $producer = EventLoop::repeat(0.1, function (): void {
            $this->redis->publish('foo', 'bar');
        });

        try {
            foreach ($iterator as $value) {
                self::assertSame('bar', $value);
                // We only need to consume a single item to confirm the subscription was not automatically cancelled.
                return;
            }

            self::fail('Subscription cancelled by destructor');
        } finally {
            EventLoop::cancel($producer);
        }
    }
}
