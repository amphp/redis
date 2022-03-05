<?php /** @noinspection PhpUnhandledExceptionInspection */

namespace Amp\Redis;

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

    private function getNextValue(Subscription $subscription): string
    {
        if (!$subscription->continue()) {
            self::fail('Expected value from redis subscription');
        }

        return $subscription->getValue();
    }

    public function testBasic(): void
    {
        $subscriber = new Subscriber(Config::fromUri($this->getUri()));

        $subscription = $subscriber->subscribe('foo');

        // Use async() to not block, because we publish in the same coroutine
        $promise = async(fn () => $this->getNextValue($subscription));

        delay(0.1); // Enter event loop so subscriber has time to connect.

        $this->redis->publish('foo', 'bar');

        delay(0.1); // Enter event loop so subscription has time to receive publish.

        $subscription->dispose();

        delay(0.1);

        $this->assertEquals('bar', $promise->await());
    }

    public function testDoubleCancel(): void
    {
        $subscriber = new Subscriber(Config::fromUri($this->getUri()));

        $subscription = $subscriber->subscribe('foo');
        $subscription->dispose();
        $subscription->dispose();

        delay(0.1); // Ensure cancel request has completed.

        $this->assertTrue(true);
    }

    public function testMulti(): void
    {
        $subscriber = new Subscriber(Config::fromUri($this->getUri()));

        $subscription1 = $subscriber->subscribe('foo');
        $subscription2 = $subscriber->subscribe('foo');

        delay(0.1); // Enter event loop so subscriber has time to connect.

        $this->redis->publish('foo', 'bar');

        $this->assertEquals('bar', $this->getNextValue($subscription1));
        $this->assertEquals('bar', $this->getNextValue($subscription2));

        $subscription1->dispose();

        $this->redis->publish('foo', 'xxx');

        $this->assertEquals('xxx', $this->getNextValue($subscription2));

        $subscription2->dispose();

        delay(0.1); // Ensure cancel request has completed.
    }

    public function testStream(): void
    {
        $subscriber = new Subscriber(Config::fromUri($this->getUri()));

        $subscription = $subscriber->subscribe('foo');

        $producer = EventLoop::repeat(0.1, function (): void {
            $this->redis->publish('foo', 'bar');
        });

        $consumed = 0;

        while ($lastResult = $this->getNextValue($subscription)) {
            $consumed++;
            if ($consumed === 3) {
                $subscription->dispose();
                break;
            }
        }

        EventLoop::cancel($producer);

        $this->assertSame(3, $consumed);
        $this->assertEquals('bar', $lastResult);

        delay(0.1); // Ensure cancel request has completed.
    }
}
