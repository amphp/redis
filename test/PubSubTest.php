<?php /** @noinspection PhpUnhandledExceptionInspection */

namespace Amp\Redis;

use Amp\Delayed;
use Amp\Loop;

class PubSubTest extends IntegrationTest
{
    public function testBasic(): \Generator
    {
        $subscriber = new SubscribeClient($this->getUri());

        /** @var Subscription $subscription */
        $subscription = yield $subscriber->subscribe('foo');

        $result = null;

        // Use callback to not block, because we publish in the same coroutine
        $subscription->advance()->onResolve(static function () use (&$result, $subscription) {
            $result = $subscription->getCurrent();
        });

        yield $this->redis->publish('foo', 'bar');
        yield new Delayed(1000);

        $subscription->cancel();

        $this->assertEquals('bar', $result);
    }

    public function testDoubleCancel(): \Generator
    {
        $subscriber = new SubscribeClient($this->getUri());

        /** @var Subscription $subscription */
        $subscription = yield $subscriber->subscribe('foo');
        $subscription->cancel();
        $subscription->cancel();

        $this->assertTrue(true);
    }

    public function testMulti(): \Generator
    {
        $subscriber = new SubscribeClient($this->getUri());

        /** @var Subscription $subscription1 */
        $subscription1 = yield $subscriber->subscribe('foo');
        /** @var Subscription $subscription2 */
        $subscription2 = yield $subscriber->subscribe('foo');

        $result1 = $result2 = null;

        $subscription1->advance()->onResolve(function ($error) use (&$result1, $subscription1) {
            $this->assertNull($error);
            $result1 = $subscription1->getCurrent();
        });

        $subscription2->advance()->onResolve(function ($error) use (&$result2, $subscription2) {
            $this->assertNull($error);
            $result2 = $subscription2->getCurrent();
        });

        yield $this->redis->publish('foo', 'bar');
        yield new Delayed(1000);

        $this->assertEquals('bar', $result1);
        $this->assertEquals('bar', $result2);

        $subscription1->cancel();

        $subscription2->advance()->onResolve(function ($error) use (&$result2, $subscription2) {
            $this->assertNull($error);
            $result2 = $subscription2->getCurrent();
        });

        yield $this->redis->publish('foo', 'xxx');
        yield new Delayed(1000);

        $this->assertEquals('bar', $result1);
        $this->assertEquals('xxx', $result2);

        $subscription2->cancel();
    }

    public function testStream(): \Generator
    {
        $subscriber = new SubscribeClient($this->getUri());

        /** @var Subscription $subscription */
        $subscription = yield $subscriber->subscribe('foo');

        $producer = Loop::repeat(1000, function () {
            yield $this->redis->publish('foo', 'bar');
        });

        $lastResult = null;
        $consumed = 0;

        while (yield $subscription->advance()) {
            $lastResult = $subscription->getCurrent();
            $consumed++;
            if ($consumed === 3) {
                $subscription->cancel();
            }
        }

        Loop::cancel($producer);

        $this->assertSame(3, $consumed);
        $this->assertEquals('bar', $lastResult);
    }
}
