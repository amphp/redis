<?php

namespace Amphp\Redis;

class ParserTest extends \PHPUnit_Framework_TestCase {
	/**
	 * @test
	 */
	function bulkString () {
		$result = null;
		$parser = new RespParser(function($resp) use (&$result) {
			$result = $resp;
		});
		$parser->append("$3\r\nfoo\r\n");

		$this->assertEquals("foo", $result);
	}

	/**
	 * @test
	 */
	function integer () {
		$result = null;
		$parser = new RespParser(function($resp) use (&$result) {
			$result = $resp;
		});
		$parser->append(":42\r\n");

		$this->assertEquals(42, $result);
	}

	/**
	 * @test
	 */
	function simpleString () {
		$result = null;
		$parser = new RespParser(function($resp) use (&$result) {
			$result = $resp;
		});
		$parser->append("+foo\r\n");

		$this->assertEquals("foo", $result);
	}

	/**
	 * @test
	 */
	function error () {
		$result = null;
		$parser = new RespParser(function($resp) use (&$result) {
			$result = $resp;
		});
		$parser->append("-ERR something went wrong :(\r\n");

		$this->assertInstanceOf("Amphp\\Redis\\RedisException", $result);
	}

	/**
	 * @test
	 */
	function arrayNull () {
		$result = null;
		$parser = new RespParser(function($resp) use (&$result) {
			$result = $resp;
		});
		$parser->append("*-1\r\n");

		$this->assertEquals(null, $result);
	}

	/**
	 * @test
	 */
	function arrayEmpty () {
		$result = null;
		$parser = new RespParser(function($resp) use (&$result) {
			$result = $resp;
		});
		$parser->append("*0\r\n");

		$this->assertEquals([], $result);
	}

	/**
	 * @test
	 */
	function arraySingle () {
		$result = null;
		$parser = new RespParser(function($resp) use (&$result) {
			$result = $resp;
		});
		$parser->append("*1\r\n+foo\r\n");

		$this->assertEquals(["foo"], $result);
	}

	/**
	 * @test
	 */
	function arrayMultiple () {
		$result = null;
		$parser = new RespParser(function($resp) use (&$result) {
			$result = $resp;
		});
		$parser->append("*3\r\n+foo\r\n:42\r\n$11\r\nHello World\r\n");

		$this->assertEquals(["foo", 42, "Hello World"], $result);
	}

	/**
	 * @test
	 */
	function arrayComplex () {
		$result = null;
		$parser = new RespParser(function($resp) use (&$result) {
			$result = $resp;
		});
		$parser->append("*3\r\n*1\r\n+foo\r\n:42\r\n*2\r\n+bar\r\n$3\r\nbaz\r\n");

		$this->assertEquals([["foo"], 42, ["bar", "baz"]], $result);
	}

	/**
	 * @test
	 */
	function arrayInnerEmpty () {
		$result = null;
		$parser = new RespParser(function($resp) use (&$result) {
			$result = $resp;
		});
		$parser->append("*1\r\n*-1\r\n");

		$this->assertEquals([null], $result);
	}

	/**
	 * @test
	 */
	function pipeline () {
		$result = null;
		$parser = new RespParser(function($resp) use (&$result) {
			$result = $resp;
		});
		$parser->append("+foo\r\n+bar\r\n");

		$this->assertEquals("bar", $result);
	}

	/**
	 * @test
	 */
	function latency () {
		$result = null;
		$parser = new RespParser(function($resp) use (&$result) {
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
	function unknownType () {
		$this->setExpectedException("Amphp\\Redis\\RedisException");
		$parser = new RespParser(function($resp) { });
		$parser->append("3$\r\nfoo\r\n");
	}
}
