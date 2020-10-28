<?php /** @noinspection PhpUnhandledExceptionInspection */

namespace Amp\Redis\Mutex;

use Amp\Redis\Config;
use Amp\Redis\IntegrationTest;
use Amp\Redis\RemoteExecutorFactory;
use function Amp\defer;
use function Amp\delay;

class MutexTest extends IntegrationTest
{
    public function testTimeout(): void
    {
        $this->setMinimumRuntime((new MutexOptions)->getLockTimeout());

        $mutex = new Mutex(new RemoteExecutorFactory(Config::fromUri($this->getUri())));

        $this->assertSame(0, $mutex->getNumberOfLocks());
        $this->assertSame(0, $mutex->getNumberOfAttempts());

        $lock1 = $mutex->acquire('foo1');

        $this->assertSame(1, $mutex->getNumberOfLocks());
        $this->assertSame(1, $mutex->getNumberOfAttempts());

        $mutex->resetStatistics();

        $this->assertSame(0, $mutex->getNumberOfLocks());
        $this->assertSame(0, $mutex->getNumberOfAttempts());

        try {
            $lock2 = $mutex->acquire('foo1');
        } catch (\Exception $e) {
            return;
        }

        $this->fail('acquire() must throw due to a timeout');
    }

    public function testFree(): void
    {
        $mutex = new Mutex(new RemoteExecutorFactory(Config::fromUri($this->getUri())));

        $lock1 = $mutex->acquire('foo2');

        defer(function () use ($lock1): void {
            delay(500);
            $lock1->release();
        });

        $mutex->acquire('foo2');

        $this->assertTrue(true);
    }

    public function testRenew(): void
    {
        $mutex = new Mutex(new RemoteExecutorFactory(Config::fromUri($this->getUri())));

        $lock1 = $mutex->acquire('foo3');

        delay(5000);

        try {
            $mutex->acquire('foo3');
        } catch (\Exception $e) {
            $this->assertSame(2, $mutex->getNumberOfLocks());
            $this->assertGreaterThan(2, $mutex->getNumberOfAttempts());

            return;
        }

        $this->fail('lock must throw');
    }
}
