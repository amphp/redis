<?php /** @noinspection PhpUnhandledExceptionInspection */

namespace Amp\Redis;

use Amp\Delayed;
use Amp\Loop;
use function Amp\async;
use function Amp\asyncCallable;
use function Amp\await;
use function Amp\delay;

class PubSubTest extends IntegrationTest
{
    public function testBasic(): void
    {
        $subscriber = new Subscriber(Config::fromUri($this->getUri()));

        $subscription = $subscriber->subscribe('foo');

        $result = null;

        // Use async() to not block, because we publish in the same coroutine
        $promise = async(fn() => $subscription->continue());

        $this->redis->publish('foo', 'bar');
        delay(1000);

        $subscription->dispose();

        delay(100);

        $this->assertEquals('bar', await($promise));
    }

    public function testDoubleCancel(): void
    {
        $subscriber = new Subscriber(Config::fromUri($this->getUri()));

        $subscription = $subscriber->subscribe('foo');
        $subscription->dispose();
        $subscription->dispose();

        delay(100); // Ensure cancel request has completed.

        $this->assertTrue(true);
    }

    public function testMulti(): void
    {
        $subscriber = new Subscriber(Config::fromUri($this->getUri()));

        $subscription1 = $subscriber->subscribe('foo');
        $subscription2 = $subscriber->subscribe('foo');

        $this->redis->publish('foo', 'bar');

        $this->assertEquals('bar', $subscription1->continue());
        $this->assertEquals('bar', $subscription2->continue());

        $subscription1->dispose();

        $this->redis->publish('foo', 'xxx');

        $this->assertEquals('xxx', $subscription2->continue());

        $subscription2->dispose();

        delay(100); // Ensure cancel request has completed.
    }

    public function testStream(): void
    {
        $subscriber = new Subscriber(Config::fromUri($this->getUri()));

        $subscription = $subscriber->subscribe('foo');

        $producer = Loop::repeat(100, asyncCallable(function (): void {
            $this->redis->publish('foo', 'bar');
        }));

        $lastResult = null;
        $consumed = 0;

        while ($lastResult = $subscription->continue()) {
            $consumed++;
            if ($consumed === 3) {
                $subscription->dispose();
                break;
            }
        }

        Loop::cancel($producer);

        $this->assertSame(3, $consumed);
        $this->assertEquals('bar', $lastResult);

        delay(100); // Ensure cancel request has completed.
    }
}
