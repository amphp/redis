<?php

namespace Amp\Redis\Mutex;

use Amp\Delayed;
use Amp\Loop;
use Amp\Redis\Client;
use Amp\Redis\RedisTest;

class MutexTest extends RedisTest
{
    public function setUp(): void
    {
        Loop::run(function () {
            $client = new Client('tcp://127.0.0.1:6379');
            yield $client->flushAll();
            yield $client->close();
        });
    }

    /**
     * @test
     */
    public function timeout()
    {
        Loop::run(function () {
            $mutex = new Mutex('tcp://127.0.0.1:6379');

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
        });
    }

    /**
     * @test
     */
    public function free()
    {
        Loop::run(function () {
            $mutex = new Mutex('tcp://127.0.0.1:6379');

            yield $mutex->lock('foo2', '123456789');

            $pause = new Delayed(500);
            $pause->onResolve(function () use ($mutex) {
                $mutex->unlock('foo2', '123456789');
            });

            yield $pause;

            yield $mutex->lock('foo2', '234567891');

            $mutex->shutdown();
            $this->assertTrue(true);
        });
    }

    /**
     * @test
     */
    public function renew()
    {
        Loop::run(function () {
            $mutex = new Mutex('tcp://127.0.0.1:6379');

            yield $mutex->lock('foo3', '123456789');

            for ($i = 0; $i < 5; $i++) {
                $pause = new Delayed(500);
                $pause->onResolve(function () use ($mutex) {
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
        });
    }
}
