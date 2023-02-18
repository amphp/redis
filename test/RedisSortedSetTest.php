<?php declare(strict_types=1);

namespace Amp\Redis;

class RedisSortedSetTest extends IntegrationTest
{
    public function testScoredSet(): void
    {
        $this->redis->flushAll();
        $set = $this->redis->getSortedSet('sorted-set-1');

        $this->assertSame(2, $set->add([
            'foo' => 1,
            'bar' => 3,
        ]));

        $this->assertSame([['foo', 1.0], ['bar', 3.0]], \iterator_to_array($set->scan()));
        $this->assertSame([['foo', 1.0]], \iterator_to_array($set->scan('f*')));
        $this->assertSame([['foo', 1.0]], \iterator_to_array($set->scan('f*', 1)));

        $this->assertSame(1.0, $set->getScore('foo'));
        $this->assertSame(3.0, $set->getScore('bar'));

        $this->assertSame(2, $set->count(0, 10));
        $this->assertSame(1, $set->count(1, 1));
        $this->assertSame(1, $set->count(3, 3));

        $this->assertSame(['foo'], $set->getRange(0, 0));
        $this->assertSame(['foo' => 1.0], $set->getRangeWithScores(0, 0));
        $this->assertSame(['foo', 'bar'], $set->getRange(0, 1));
        $this->assertSame(['bar'], $set->getRange(1, 2));

        $this->assertSame(['foo'], $set->getRangeByScore(RangeBoundary::inclusive(1), RangeBoundary::inclusive(2)));
        $this->assertSame(['foo' => 1.0], $set->getRangeByScoreWithScores(RangeBoundary::inclusive(1), RangeBoundary::exclusive(3)));
        $this->assertSame(['foo', 'bar'], $set->getRangeByScore(RangeBoundary::negativeInfinity(), RangeBoundary::inclusive(3)));
        $this->assertSame(['foo'], $set->getRangeByScore(RangeBoundary::inclusive(1), RangeBoundary::exclusive(3)));

        $this->assertSame(['bar', 'foo'], $set->getRange(0, 1, (new RangeOptions())->withReverseOrder()));
        $this->assertSame(['bar' => 3.0], $set->getRangeWithScores(0, 0, (new RangeOptions())->withReverseOrder()));
        $this->assertSame(['foo'], $set->getRange(1, 2, (new RangeOptions())->withReverseOrder()));

        $this->assertSame(['foo'], $set->getRangeByScore(RangeBoundary::inclusive(2), RangeBoundary::inclusive(1), (new RangeOptions())->withReverseOrder()));
        $this->assertSame(['bar' => 3.0, 'foo' => 1.0], $set->getRangeByScoreWithScores(RangeBoundary::positiveInfinity(), RangeBoundary::inclusive(1), (new RangeOptions())->withReverseOrder()));
        $this->assertSame(['bar', 'foo'], $set->getRangeByScore(RangeBoundary::inclusive(3), RangeBoundary::inclusive(1), (new RangeOptions())->withReverseOrder()));


        $this->assertSame(0, $set->getRank('foo'));
        $this->assertSame(1, $set->getRank('bar'));

        $this->assertSame(1, $set->getReversedRank('foo'));
        $this->assertSame(0, $set->getReversedRank('bar'));

        $this->assertSame(1, $set->remove('foo'));
        $this->assertSame(0, $set->getRank('bar'));
    }

    public function testRemove(): void
    {
        $this->redis->flushAll();
        $set = $this->redis->getSortedSet('sorted-set-1');

        $this->assertSame(3, $set->add([
            'foo' => 1.1,
            'bar' => 2.2,
            'baz' => 3.3,
        ]));

        $set->removeRangeByScore(RangeBoundary::exclusive(2.2), RangeBoundary::positiveInfinity());
        $this->assertSame(['foo', 'bar'], $set->getRangeByScore(RangeBoundary::negativeInfinity(), RangeBoundary::positiveInfinity()));
    }

    public function testLexSet(): void
    {
        $this->redis->flushAll();
        $set = $this->redis->getSortedSet('sorted-set-1');

        $this->assertSame(4, $set->add([
            'a' => 0,
            'b' => 0,
            'c' => 0,
            'd' => 0,
        ]));

        $this->assertSame(['a', 'b', 'c'], $set->getRangeLexicographically('[a', '[c'));
        $this->assertSame(['b', 'c'], $set->getRangeLexicographically('(a', '[c'));
        $this->assertSame(['a', 'b', 'c'], $set->getRangeLexicographically('-', '(d'));
    }

}
