<?php declare(strict_types=1);

namespace Amp\Redis;

use function Amp\async;

class CloseTest extends IntegrationTest
{
    public function testReconnect(): void
    {
        $redis = $this->createInstance();
        $this->assertEquals('PONG', $redis->echo('PONG'));
        $redis->quit();
        $this->assertEquals('PONG', $redis->echo('PONG'));
    }

    public function testReconnect2(): void
    {
        $redis = $this->createInstance();
        $this->assertEquals('PONG', $redis->echo('PONG'));
        $quitPromise = async(fn () => $redis->quit());
        $promise = async(fn () => $redis->echo('PONG'));
        $quitPromise->await();
        $this->assertEquals('PONG', $promise->await());
    }
}
