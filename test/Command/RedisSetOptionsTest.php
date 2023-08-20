<?php declare(strict_types=1);

namespace Amp\Redis\Command;

use Amp\Redis\Command\Option\SetOptions;
use PHPUnit\Framework\TestCase;

class RedisSetOptionsTest extends TestCase
{
    public function test(): void
    {
        $options = new SetOptions;

        $this->assertSame([], $options->toQuery());
        $this->assertSame(['EX', 3], $options->withTtl(3)->toQuery());
        $this->assertSame(['PX', 3], $options->withTtlInMillis(3)->toQuery());
        $this->assertSame(['PX', 3, 'XX'], $options->withTtlInMillis(3)->withoutCreation()->toQuery());
        $this->assertSame(['PX', 3, 'NX'], $options->withTtlInMillis(3)->withoutOverwrite()->toQuery());
        $this->assertSame(
            ['PX', 3, 'NX'],
            $options->withTtlInMillis(3)->withoutCreation()->withoutOverwrite()->toQuery()
        );
    }
}
