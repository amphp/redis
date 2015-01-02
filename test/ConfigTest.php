<?php

namespace Amphp\Redis;

class ConfigTest extends \PHPUnit_Framework_TestCase {
	/**
	 * @test
	 */
	function host () {
		$config = new ConnectionConfig;
		$config->setHost("example.com");

		$this->assertEquals("example.com", $config->getHost());
	}

	/**
	 * @test
	 */
	function port () {
		$config = new ConnectionConfig;
		$config->setPort(12345);

		$this->assertEquals(12345, $config->getPort());
	}

	/**
	 * @test
	 */
	function password () {
		$config = new ConnectionConfig;
		$config->setPasswort("secret");

		$this->assertTrue($config->hasPassword());
		$this->assertEquals("secret", $config->getPassword());
	}
}
