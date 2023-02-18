<?php

namespace Amp\Redis;

use Amp\Iterator;

class RedisSortedSetTest extends IntegrationTest
{
    public function testScoredSet(): \Generator
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
        $this->assertSame(['foo' => 1.0], yield $set->getRangeWithScores(0, 0));
        $this->assertSame(['foo', 'bar'], yield $set->getRange(0, 1));
        $this->assertSame(['bar'], yield $set->getRange(1, 2));

        $this->assertSame(['foo'], yield $set->getRangeByScore(RangeBoundary::inclusive(1), RangeBoundary::inclusive(2)));
        $this->assertSame(['foo' => 1.0], yield $set->getRangeByScoreWithScores(RangeBoundary::inclusive(1), RangeBoundary::exclusive(3)));
        $this->assertSame(['foo', 'bar'], yield $set->getRangeByScore(RangeBoundary::negativeInfinity(), RangeBoundary::inclusive(3)));
        $this->assertSame(['foo'], yield $set->getRangeByScore(RangeBoundary::inclusive(1), RangeBoundary::exclusive(3)));


        $this->assertSame(['bar', 'foo'], yield $set->getRange(0, 1, (new RangeOptions())->withReverseOrder()));
        $this->assertSame(['bar' => 3.0], yield $set->getRangeWithScores(0, 0, (new RangeOptions())->withReverseOrder()));
        $this->assertSame(['foo'], yield $set->getRange(1, 2, (new RangeOptions())->withReverseOrder()));

        $this->assertSame(['foo'], yield $set->getRangeByScore(RangeBoundary::inclusive(2), RangeBoundary::inclusive(1), (new RangeOptions())->withReverseOrder()));
        $this->assertSame(['bar' => 3.0, 'foo' => 1.0], yield $set->getRangeByScoreWithScores(RangeBoundary::positiveInfinity(), RangeBoundary::inclusive(1), (new RangeOptions())->withReverseOrder()));
        $this->assertSame(['bar', 'foo'], yield $set->getRangeByScore(RangeBoundary::inclusive(3), RangeBoundary::inclusive(1), (new RangeOptions())->withReverseOrder()));

        $this->assertSame(0, yield $set->getRank('foo'));
        $this->assertSame(1, yield $set->getRank('bar'));

        $this->assertSame(1, yield $set->getReversedRank('foo'));
        $this->assertSame(0, yield $set->getReversedRank('bar'));

        $this->assertSame(1, yield $set->remove('foo'));
        $this->assertSame(0, yield $set->getRank('bar'));
    }

    public function testRemove(): \Generator
    {
        $this->redis->flushAll();
        $set = $this->redis->getSortedSet('sorted-set-1');

        $this->assertSame(3, yield $set->add([
            'foo' => 1.1,
            'bar' => 2.2,
            'baz' => 3.3,
        ]));

        yield $set->removeRangeByScore(RangeBoundary::exclusive(2.2), RangeBoundary::positiveInfinity());
        $this->assertSame(['foo', 'bar'], yield $set->getRangeByScore(RangeBoundary::negativeInfinity(), RangeBoundary::positiveInfinity()));
    }

    public function testLexSet(): \Generator
    {
        $this->redis->flushAll();
        $set = $this->redis->getSortedSet('sorted-set-1');

        $this->assertSame(4, yield $set->add([
            'a' => 0,
            'b' => 0,
            'c' => 0,
            'd' => 0,
        ]));

        $this->assertSame(['a', 'b', 'c'], yield $set->getRangeLexicographically('[a', '[c'));
        $this->assertSame(['b', 'c'], yield $set->getRangeLexicographically('(a', '[c'));
        $this->assertSame(['a', 'b', 'c'], yield $set->getRangeLexicographically('-', '(d'));
    }
}
