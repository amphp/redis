<?php

namespace Amp\Redis;

use PHPUnit\Framework\TestCase;

class SetOptionsTest extends TestCase
{
    public function test(): void
    {
        $options = new SetOptions;

        $this->assertSame([], $options->toSetQuery());
        $this->assertSame(['EX', 3], $options->withTtl(3)->toSetQuery());
        $this->assertSame(['PX', 3], $options->withTtlInMillis(3)->toSetQuery());
        $this->assertSame(['PX', 3, 'XX'], $options->withTtlInMillis(3)->withoutCreation()->toSetQuery());
        $this->assertSame(['PX', 3, 'NX'], $options->withTtlInMillis(3)->withoutOverwrite()->toSetQuery());
        $this->assertSame(
            ['PX', 3, 'NX'],
            $options->withTtlInMillis(3)->withoutCreation()->withoutOverwrite()->toSetQuery()
        );
    }
}
