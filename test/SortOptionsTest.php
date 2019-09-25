<?php

namespace Amp\Redis;

use PHPUnit\Framework\TestCase;

class SortOptionsTest extends TestCase
{
    public function test(): void
    {
        $options = new SortOptions;

        $this->assertFalse($options->hasLimit());
        $this->assertTrue($options->withLimit(0, 100)->hasLimit());
        $this->assertFalse($options->withLimit(0, 100)->withoutLimit()->hasLimit());
        $this->assertSame(0, $options->withLimit(0, 100)->getOffset());
        $this->assertSame(100, $options->withLimit(0, 100)->getCount());

        $this->assertFalse($options->hasPattern());
        $this->assertTrue($options->withPattern('test*')->hasPattern());
        $this->assertFalse($options->withPattern('test*')->withoutPattern()->hasPattern());
        $this->assertSame('test*', $options->withPattern('test*')->getPattern());

        $this->assertTrue($options->isAscending());
        $this->assertFalse($options->isDescending());

        $this->assertTrue($options->withAscendingOrder()->isAscending());
        $this->assertTrue($options->withDescendingOrder()->isDescending());

        $this->assertFalse($options->withAscendingOrder()->isDescending());
        $this->assertFalse($options->withDescendingOrder()->isAscending());

        $this->assertFalse($options->isLexicographicSorting());
        $this->assertTrue($options->withLexicographicSorting()->isLexicographicSorting());
        $this->assertFalse($options->withLexicographicSorting()->withNumericSorting()->isLexicographicSorting());
    }
}
