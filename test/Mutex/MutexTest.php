<?php /** @noinspection PhpUnhandledExceptionInspection */

namespace Amp\Redis\Mutex;

use Amp\Delayed;
use Amp\Redis\IntegrationTest;
use Amp\Redis\RemoteExecutorFactory;

class MutexTest extends IntegrationTest
{
    public function testTimeout(): \Generator
    {
        $mutex = new Mutex(new RemoteExecutorFactory($this->getUri()));

        yield $mutex->lock('foo1', '123456789');

        try {
            yield $mutex->lock('foo1', '234567891');
        } catch (\Exception $e) {
            $this->assertTrue(true);
            return;
        } finally {
            $mutex->shutdown();
        }

        $this->fail('lock must throw');
    }

    public function testFree(): \Generator
    {
        $mutex = new Mutex(new RemoteExecutorFactory($this->getUri()));

        yield $mutex->lock('foo2', '123456789');

        $pause = new Delayed(500);
        $pause->onResolve(static function () use ($mutex) {
            $mutex->unlock('foo2', '123456789');
        });

        yield $pause;

        yield $mutex->lock('foo2', '234567891');

        $mutex->shutdown();
        $this->assertTrue(true);
    }

    public function testRenew(): \Generator
    {
        $mutex = new Mutex(new RemoteExecutorFactory($this->getUri()));

        yield $mutex->lock('foo3', '123456789');

        for ($i = 0; $i < 5; $i++) {
            $pause = new Delayed(500);
            $pause->onResolve(static function () use ($mutex) {
                $mutex->renew('foo3', '123456789');
            });

            yield $pause;
        }

        try {
            yield $mutex->lock('foo3', '234567891');
        } catch (\Exception $e) {
            $this->assertTrue(true);
            return;
        } finally {
            $mutex->shutdown();
        }

        $this->fail('lock must throw');
    }
}
