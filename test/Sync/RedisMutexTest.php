<?php declare(strict_types=1);

namespace Amp\Redis\Sync;

use Amp\Redis\IntegrationTest;
use Amp\Redis\RedisConfig;
use Amp\Redis\RemoteExecutorFactory;
use Revolt\EventLoop;
use function Amp\delay;

class RedisMutexTest extends IntegrationTest
{
    private RedisMutexOptions $options;

    private RedisMutex $mutex;

    protected function setUp(): void
    {
        parent::setUp();
        $this->options = (new RedisMutexOptions())->withLockTimeout(1);
        $executorFactory = new RemoteExecutorFactory(RedisConfig::fromUri($this->getUri()));

        $this->mutex = new RedisMutex($executorFactory->createQueryExecutor(), $this->options);
    }

    public function testTimeout(): void
    {
        $this->setMinimumRuntime($this->options->getLockTimeout());

        $this->assertSame(0, $this->mutex->getNumberOfLocks());
        $this->assertSame(0, $this->mutex->getNumberOfAttempts());

        $lock1 = $this->mutex->acquire('foo1');

        $this->assertSame(1, $this->mutex->getNumberOfLocks());
        $this->assertSame(1, $this->mutex->getNumberOfAttempts());

        $this->mutex->resetStatistics();

        $this->assertSame(0, $this->mutex->getNumberOfLocks());
        $this->assertSame(0, $this->mutex->getNumberOfAttempts());

        try {
            $lock2 = $this->mutex->acquire('foo1');
        } catch (\Exception $e) {
            return;
        }

        $this->fail('acquire() must throw due to a timeout');
    }

    public function testFree(): void
    {
        $lock1 = $this->mutex->acquire('foo2');

        EventLoop::queue(function () use ($lock1): void {
            delay(0.5);
            $lock1->release();
        });

        $this->mutex->acquire('foo2');

        $this->assertTrue(true);
    }

    public function testRenew(): void
    {
        $lock1 = $this->mutex->acquire('foo3');

        delay($this->options->getLockTimeout() / 2);

        try {
            $this->mutex->acquire('foo3');
        } catch (\Exception $e) {
            $this->assertSame(2, $this->mutex->getNumberOfLocks());
            $this->assertGreaterThan(2, $this->mutex->getNumberOfAttempts());

            return;
        }

        $this->fail('lock must throw');
    }
}
