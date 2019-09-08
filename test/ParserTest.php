<?php /** @noinspection PhpUnhandledExceptionInspection */

namespace Amp\Redis;

use PHPUnit\Framework\TestCase;

class ParserTest extends TestCase
{
    public function testBulkString(): void
    {
        $result = null;
        $parser = new RespParser(static function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("$3\r\nfoo\r\n");

        $this->assertEquals('foo', $result);
    }

    public function testInteger(): void
    {
        $result = null;
        $parser = new RespParser(static function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append(":42\r\n");

        $this->assertEquals(42, $result);
    }

    public function testSimpleString(): void
    {
        $result = null;
        $parser = new RespParser(static function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("+foo\r\n");

        $this->assertEquals('foo', $result);
    }

    public function testError(): void
    {
        $result = null;
        $parser = new RespParser(static function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("-ERR something went wrong :(\r\n");

        $this->assertInstanceOf(QueryException::class, $result);
    }

    public function testStringNull(): void
    {
        $result = false;
        $parser = new RespParser(static function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("$-1\r\n");

        $this->assertNull($result);
    }

    public function testPipeline(): void
    {
        $result = null;
        $parser = new RespParser(static function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("+foo\r\n+bar\r\n");

        $this->assertEquals('bar', $result);
    }

    public function testLatency(): void
    {
        $result = null;
        $parser = new RespParser(static function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("$3\r");
        $this->assertNull($result);
        $parser->append("\nfoo\r");
        $this->assertNull($result);
        $parser->append("\n");
        $this->assertEquals('foo', $result);
    }

    public function testArrayNull(): void
    {
        $result = false;
        $parser = new RespParser(static function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("*-1\r\n");

        $this->assertNull($result);
    }

    public function testArrayEmpty(): void
    {
        $result = null;
        $parser = new RespParser(static function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("*0\r\n");

        $this->assertEquals([], $result);
    }

    public function testArraySingle(): void
    {
        $result = null;
        $parser = new RespParser(static function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("*1\r\n+foo\r\n");

        $this->assertEquals(['foo'], $result);
    }

    public function testArrayMultiple(): void
    {
        $result = null;
        $parser = new RespParser(static function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("*3\r\n+foo\r\n:42\r\n$11\r\nHello World\r\n");

        $this->assertEquals(['foo', 42, 'Hello World'], $result);
    }

    public function testArrayComplex(): void
    {
        $result = null;
        $parser = new RespParser(static function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("*3\r\n*1\r\n+foo\r\n:42\r\n*2\r\n+bar\r\n$3\r\nbaz\r\n");

        $this->assertEquals([['foo'], 42, ['bar', 'baz']], $result);
    }

    public function testArrayInnerEmpty(): void
    {
        $result = null;
        $parser = new RespParser(static function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("*1\r\n*-1\r\n");

        $this->assertEquals([null], $result);
    }

    /**
     * @see https://github.com/amphp/redis/commit/a495189735412c8962b219b6633685ddca84040c
     */
    public function testArrayPipeline(): void
    {
        $result = null;
        $parser = new RespParser(static function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("*1\r\n+foo\r\n*1\r\n+bar\r\n");

        $this->assertEquals(['bar'], $result);
    }

    public function testUnknownType(): void
    {
        $this->expectException(ParserException::class);

        $parser = new RespParser(static function ($resp) {
            // do nothing
        });

        $parser->append("3$\r\nfoo\r\n");
    }
}
