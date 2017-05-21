<?php

namespace Amp\Redis;

use PHPUnit\Framework\TestCase;

class ParserTest extends TestCase {
    /**
     * @test
     */
    public function bulkString() {
        $result = null;
        $parser = new RespParser(function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("$3\r\nfoo\r\n");

        $this->assertEquals("foo", $result);
    }

    /**
     * @test
     */
    public function integer() {
        $result = null;
        $parser = new RespParser(function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append(":42\r\n");

        $this->assertEquals(42, $result);
    }

    /**
     * @test
     */
    public function simpleString() {
        $result = null;
        $parser = new RespParser(function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("+foo\r\n");

        $this->assertEquals("foo", $result);
    }

    /**
     * @test
     */
    public function error() {
        $result = null;
        $parser = new RespParser(function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("-ERR something went wrong :(\r\n");

        $this->assertInstanceOf("Amp\\Redis\\QueryException", $result);
    }

    /**
     * @test
     */
    public function stringNull() {
        $result = false;
        $parser = new RespParser(function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("$-1\r\n");

        $this->assertNull($result);
    }

    /**
     * @test
     */
    public function pipeline() {
        $result = null;
        $parser = new RespParser(function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("+foo\r\n+bar\r\n");

        $this->assertEquals("bar", $result);
    }

    /**
     * @test
     */
    public function latency() {
        $result = null;
        $parser = new RespParser(function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("$3\r");
        $this->assertEquals(null, $result);
        $parser->append("\nfoo\r");
        $this->assertEquals(null, $result);
        $parser->append("\n");
        $this->assertEquals("foo", $result);
    }

    /**
     * @test
     */
    public function arrayNull() {
        $result = false;
        $parser = new RespParser(function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("*-1\r\n");

        $this->assertNull($result);
    }

    /**
     * @test
     */
    public function arrayEmpty() {
        $result = null;
        $parser = new RespParser(function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("*0\r\n");

        $this->assertEquals([], $result);
    }

    /**
     * @test
     */
    public function arraySingle() {
        $result = null;
        $parser = new RespParser(function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("*1\r\n+foo\r\n");

        $this->assertEquals(["foo"], $result);
    }

    /**
     * @test
     */
    public function arrayMultiple() {
        $result = null;
        $parser = new RespParser(function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("*3\r\n+foo\r\n:42\r\n$11\r\nHello World\r\n");

        $this->assertEquals(["foo", 42, "Hello World"], $result);
    }

    /**
     * @test
     */
    public function arrayComplex() {
        $result = null;
        $parser = new RespParser(function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("*3\r\n*1\r\n+foo\r\n:42\r\n*2\r\n+bar\r\n$3\r\nbaz\r\n");

        $this->assertEquals([["foo"], 42, ["bar", "baz"]], $result);
    }

    /**
     * @test
     */
    public function arrayInnerEmpty() {
        $result = null;
        $parser = new RespParser(function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("*1\r\n*-1\r\n");

        $this->assertEquals([null], $result);
    }

    /**
     * @test
     * @see https://github.com/amphp/redis/commit/a495189735412c8962b219b6633685ddca84040c
     */
    public function arrayPipeline() {
        $result = null;
        $parser = new RespParser(function ($resp) use (&$result) {
            $result = $resp;
        });
        $parser->append("*1\r\n+foo\r\n*1\r\n+bar\r\n");

        $this->assertEquals(["bar"], $result);
    }

    /**
     * @test
     * @expectedException \Amp\Redis\ParserException
     */
    public function unknownType() {
        $parser = new RespParser(function ($resp) {
        });
        $parser->append("3$\r\nfoo\r\n");
    }
}
