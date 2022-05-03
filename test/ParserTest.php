<?php

namespace Amp\Redis;

use Amp\Pipeline\ConcurrentIterator;
use Amp\Pipeline\Queue;
use Amp\Redis\Connection\RespParser;
use Amp\Redis\Connection\RespPayload;
use PHPUnit\Framework\TestCase;

class ParserTest extends TestCase
{
    private Queue $queue;
    private RespParser $parser;
    private ConcurrentIterator $iterator;

    public function setUp(): void
    {
        parent::setUp();

        $this->queue = new Queue(10);
        $this->parser = new RespParser($this->queue);
        $this->iterator = $this->queue->iterate();
    }

    public function tearDown(): void
    {
        parent::tearDown();

        $this->queue->complete();
    }

    private function getNextValue(): mixed
    {
        if (!$this->iterator->continue()) {
            self::fail('No next value available');
        }

        $payload = $this->iterator->getValue();

        self::assertInstanceOf(RespPayload::class, $payload);

        return $payload->unwrap();
    }

    public function testBulkString(): void
    {
        $this->parser->append("$3\r\nfoo\r\n");

        $this->assertEquals('foo', $this->getNextValue());
    }

    public function testInteger(): void
    {
        $this->parser->append(":42\r\n");

        $this->assertEquals(42, $this->getNextValue());
    }

    public function testSimpleString(): void
    {
        $this->parser->append("+foo\r\n");

        $this->assertEquals('foo', $this->getNextValue());
    }

    public function testError(): void
    {
        $this->parser->append("-ERR something went wrong :(\r\n");

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('ERR something went wrong :(');

        $this->getNextValue();
    }

    public function testStringNull(): void
    {
        $this->parser->append("$-1\r\n");

        $this->assertNull($this->getNextValue());
    }

    public function testPipeline(): void
    {
        $this->parser->append("+foo\r\n+bar\r\n");

        $this->assertEquals('foo', $this->getNextValue());
        $this->assertEquals('bar', $this->getNextValue());
    }

    public function testLatency(): void
    {
        $this->parser->append("$3\r");
        $this->parser->append("\nfoo\r");
        $this->parser->append("\n");
        $this->assertEquals('foo', $this->getNextValue());
    }

    public function testArrayNull(): void
    {
        $this->parser->append("*-1\r\n");

        $this->assertNull($this->getNextValue());
    }

    public function testArrayEmpty(): void
    {
        $this->parser->append("*0\r\n");

        $this->assertEquals([], $this->getNextValue());
    }

    public function testArraySingle(): void
    {
        $this->parser->append("*1\r\n+foo\r\n");

        $this->assertEquals(['foo'], $this->getNextValue());
    }

    public function testArrayMultiple(): void
    {
        $this->parser->append("*3\r\n+foo\r\n:42\r\n$11\r\nHello World\r\n");

        $this->assertEquals(['foo', 42, 'Hello World'], $this->getNextValue());
    }

    public function testArrayComplex(): void
    {
        $this->parser->append("*3\r\n*1\r\n+foo\r\n:42\r\n*2\r\n+bar\r\n$3\r\nbaz\r\n");

        $this->assertEquals([['foo'], 42, ['bar', 'baz']], $this->getNextValue());
    }

    public function testArrayInnerEmpty(): void
    {
        $this->parser->append("*1\r\n*-1\r\n");

        $this->assertEquals([null], $this->getNextValue());
    }

    /**
     * @see https://github.com/amphp/redis/commit/a495189735412c8962b219b6633685ddca84040c
     */
    public function testArrayPipeline(): void
    {
        $this->parser->append("*1\r\n+foo\r\n*1\r\n+bar\r\n");

        $this->assertEquals(['foo'], $this->getNextValue());
        $this->assertEquals(['bar'], $this->getNextValue());
    }

    public function testUnknownType(): void
    {
        $this->expectException(ParserException::class);

        $this->parser->append("3$\r\nfoo\r\n");
    }
}
