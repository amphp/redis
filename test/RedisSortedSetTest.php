<?php

namespace Amp\Redis;

use Amp\Iterator;

class RedisSortedSetTest extends IntegrationTest
{
    public function test(): \Generator
    {
        $this->redis->flushAll();
        $set = $this->redis->getSortedSet('sorted-set-1');

        $this->assertSame(2, yield $set->add([
            'foo' => 1,
            'bar' => 3,
        ]));

        $this->assertSame([['foo', 1.0], ['bar', 3.0]], yield Iterator\toArray($set->scan()));
        $this->assertSame([['foo', 1.0]], yield Iterator\toArray($set->scan('f*')));
        $this->assertSame([['foo', 1.0]], yield Iterator\toArray($set->scan('f*', 1)));

        $this->assertSame(1.0, yield $set->getScore('foo'));
        $this->assertSame(3.0, yield $set->getScore('bar'));

        $this->assertSame(2, yield $set->count(0, 10));
        $this->assertSame(1, yield $set->count(1, 1));
        $this->assertSame(1, yield $set->count(3, 3));

        $this->assertSame(['foo'], yield $set->getRange(0, 0));
        $this->assertSame(['foo' => 1.0], yield $set->getRange(0, 0, (new RangeOptions())->withScores()));
        $this->assertSame(['foo', 'bar'], yield $set->getRange(0, 1));
        $this->assertSame(['bar'], yield $set->getRange(1, 2));

        $this->assertSame(['foo'], yield $set->getRangeByScore(1, 2));
        $this->assertSame(['foo' => 1.0], yield $set->getRangeByScore(1, 2, (new RangeByScoreOptions())->withScores()));
        $this->assertSame(['foo', 'bar'], yield $set->getRangeByScore(1, 4));

        $this->assertSame(['bar', 'foo'], yield $set->getReverseRange(0, 1));
        $this->assertSame(['bar' => 3.0], yield $set->getReverseRange(0, 0, (new RangeOptions())->withScores()));
        $this->assertSame(['foo'], yield $set->getReverseRange(1, 2));

        $this->assertSame(['foo'], yield $set->getReverseRangeByScore(2, 1));
        $this->assertSame(['bar' => 3.0, 'foo' => 1.0], yield $set->getReverseRangeByScore(4, 1, (new RangeByScoreOptions())->withScores()));
        $this->assertSame(['bar', 'foo'], yield $set->getReverseRangeByScore(4, 1));

        $this->assertSame(0, yield $set->getRank('foo'));
        $this->assertSame(1, yield $set->getRank('bar'));

        $this->assertSame(1, yield $set->getReversedRank('foo'));
        $this->assertSame(0, yield $set->getReversedRank('bar'));

        $this->assertSame(1, yield $set->remove('foo'));
        $this->assertSame(0, yield $set->getRank('bar'));
    }
}
