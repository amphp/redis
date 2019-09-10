<?php /** @noinspection PhpUnhandledExceptionInspection */

namespace Amp\Redis;

use Amp\Delayed;
use Amp\Loop;

class PubSubTest extends IntegrationTest
{
    public function testBasic(): \Generator
    {
        $subscriber = new Subscriber(Config::fromUri($this->getUri()));

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
        $subscriber = new Subscriber(Config::fromUri($this->getUri()));

        /** @var Subscription $subscription */
        $subscription = yield $subscriber->subscribe('foo');
        $subscription->cancel();
        $subscription->cancel();

        $this->assertTrue(true);
    }

    public function testMulti(): \Generator
    {
        $subscriber = new Subscriber(Config::fromUri($this->getUri()));

        /** @var Subscription $subscription1 */
        $subscription1 = yield $subscriber->subscribe('foo');
        /** @var Subscription $subscription2 */
        $subscription2 = yield $subscriber->subscribe('foo');

        yield $this->redis->publish('foo', 'bar');

        yield $subscription1->advance();
        yield $subscription2->advance();

        $this->assertEquals('bar', $subscription1->getCurrent());
        $this->assertEquals('bar', $subscription2->getCurrent());

        $subscription1->cancel();

        yield $this->redis->publish('foo', 'xxx');

        yield $subscription2->advance();

        $this->assertEquals('bar', $subscription1->getCurrent());
        $this->assertEquals('xxx', $subscription2->getCurrent());

        $subscription2->cancel();
    }

    public function testStream(): \Generator
    {
        $subscriber = new Subscriber(Config::fromUri($this->getUri()));

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
