<?php /** @noinspection PhpUnhandledExceptionInspection */

namespace Amp\Redis\Mutex;

use Amp\Delayed;
use Amp\Redis\Config;
use Amp\Redis\IntegrationTest;
use Amp\Redis\RemoteExecutorFactory;
use Amp\Sync\Lock;
use function Amp\delay;

class MutexTest extends IntegrationTest
{
    public function testTimeout(): \Generator
    {
        $this->setMinimumRuntime((new MutexOptions)->getLockTimeout());

        $mutex = new Mutex(new RemoteExecutorFactory(Config::fromUri($this->getUri())));

        $this->assertSame(0, $mutex->getNumberOfLocks());
        $this->assertSame(0, $mutex->getNumberOfAttempts());

        $lock1 = yield $mutex->acquire('foo1');

        $this->assertSame(1, $mutex->getNumberOfLocks());
        $this->assertSame(1, $mutex->getNumberOfAttempts());

        $mutex->resetStatistics();

        $this->assertSame(0, $mutex->getNumberOfLocks());
        $this->assertSame(0, $mutex->getNumberOfAttempts());

        try {
            $lock2 = yield $mutex->acquire('foo1');
        } catch (\Exception $e) {
            return;
        }

        $this->fail('acquire() must throw due to a timeout');
    }

    public function testFree(): \Generator
    {
        $mutex = new Mutex(new RemoteExecutorFactory(Config::fromUri($this->getUri())));

        /** @var Lock $lock1 */
        $lock1 = yield $mutex->acquire('foo2');

        $pause = new Delayed(500);
        $pause->onResolve(static function () use ($lock1) {
            $lock1->release();
        });

        yield $pause;

        yield $mutex->acquire('foo2');

        $this->assertTrue(true);
    }

    public function testRenew(): \Generator
    {
        $mutex = new Mutex(new RemoteExecutorFactory(Config::fromUri($this->getUri())));

        $lock1 = yield $mutex->acquire('foo3');

        yield delay(5000);

        try {
            yield $mutex->acquire('foo3');
        } catch (\Exception $e) {
            $this->assertSame(2, $mutex->getNumberOfLocks());
            $this->assertGreaterThan(2, $mutex->getNumberOfAttempts());

            return;
        }

        $this->fail('lock must throw');
    }
}
