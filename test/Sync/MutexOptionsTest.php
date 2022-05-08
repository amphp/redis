<?php

namespace Amp\Redis\Sync;

use PHPUnit\Framework\TestCase;

class MutexOptionsTest extends TestCase
{
    public function testWithKeyPrefix(): void
    {
        $options = new RedisMutexOptions;

        $this->assertSame('', $options->getKeyPrefix());
        $this->assertSame('foo:', $options->withKeyPrefix('foo:')->getKeyPrefix());
    }

    public function testWithLockTimeout(): void
    {
        $options = new RedisMutexOptions;

        $this->assertSame(10.0, $options->getLockTimeout());
        $this->assertSame(1.0, $options->withLockTimeout(1)->getLockTimeout());
    }

    public function testWithLockExpiration(): void
    {
        $options = new RedisMutexOptions;

        $this->assertSame(3.0, $options->getLockExpiration());
        $this->assertSame(1.0, $options->withLockExpiration(1)->getLockExpiration());
    }

    public function testWithLockRenewInterval(): void
    {
        $options = new RedisMutexOptions;

        $this->assertSame(1.0, $options->getLockRenewInterval());
        $this->assertSame(1.0, $options->withLockRenewInterval(1)->getLockRenewInterval());
    }
}
