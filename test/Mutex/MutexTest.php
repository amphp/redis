<?php

namespace Amp\Redis\Mutex;

use Amp\Redis\RedisConfig;
use Amp\Redis\IntegrationTest;
use Amp\Redis\RemoteExecutorFactory;
use Revolt\EventLoop;
use function Amp\delay;

class MutexTest extends IntegrationTest
{
    private MutexOptions $options;

    protected function setUp(): void
    {
        parent::setUp();
        $this->options = (new MutexOptions())->withLockTimeout(1);
    }

    public function testTimeout(): void
    {
        $this->setMinimumRuntime($this->options->getLockTimeout());

        $mutex = new Mutex(new RemoteExecutorFactory(RedisConfig::fromUri($this->getUri())), $this->options);

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
        $mutex = new Mutex(new RemoteExecutorFactory(RedisConfig::fromUri($this->getUri())), $this->options);

        $lock1 = $mutex->acquire('foo2');

        EventLoop::queue(function () use ($lock1): void {
            delay(0.5);
            $lock1->release();
        });

        $mutex->acquire('foo2');

        $this->assertTrue(true);
    }

    public function testRenew(): void
    {
        $mutex = new Mutex(new RemoteExecutorFactory(RedisConfig::fromUri($this->getUri())), $this->options);

        $lock1 = $mutex->acquire('foo3');

        delay($this->options->getLockTimeout() / 2);

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
