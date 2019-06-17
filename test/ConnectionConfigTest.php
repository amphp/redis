<?php declare(strict_types=1);

namespace Amp\Redis;

use PHPUnit\Framework\TestCase;

class ConnectionConfigTest extends TestCase
{
    /**
     * @test
     */
    public function parseTcpUri()
    {
        $connectionConfig = ConnectionConfig::parse("tcp://127.0.0.1:25325");
        $this->assertEquals("tcp://127.0.0.1:25325", $connectionConfig->getUri());
        $this->assertFalse($connectionConfig->hasPassword());
        $this->assertEquals(0, $connectionConfig->getDatabase());
        $this->assertEquals(5000, $connectionConfig->getTimeout());
    }

    /**
     * @test
     */
    public function parseTcpUriWithTimeout()
    {
        $connectionConfig = ConnectionConfig::parse("tcp://127.0.0.1:25325?timeout=1000");
        $this->assertEquals("tcp://127.0.0.1:25325", $connectionConfig->getUri());
        $this->assertFalse($connectionConfig->hasPassword());
        $this->assertEquals(0, $connectionConfig->getDatabase());
        $this->assertEquals(1000, $connectionConfig->getTimeout());
    }

    /**
     * @test
     */
    public function parseTcpUriWithDatabase()
    {
        $connectionConfig = ConnectionConfig::parse("tcp://127.0.0.1:25325?database=1");
        $this->assertEquals("tcp://127.0.0.1:25325", $connectionConfig->getUri());
        $this->assertFalse($connectionConfig->hasPassword());
        $this->assertEquals(1, $connectionConfig->getDatabase());
        $this->assertEquals(5000, $connectionConfig->getTimeout());
    }

    /**
     * @test
     * @expectedException \Amp\Redis\ConnectionConfigException
     */
    public function failParseTcpUriWithNonNumericDatabase()
    {
        ConnectionConfig::parse("tcp://127.0.0.1:25325?database=db");
    }

    /**
     * @test
     * @expectedException \Amp\Redis\ConnectionConfigException
     */
    public function failParseTcpUriWithNegativeDatabase()
    {
        ConnectionConfig::parse("tcp://127.0.0.1:25325?database=-1");
    }

    /**
     * @test
     */
    public function parseTcpUriWithPassword()
    {
        $connectionConfig = ConnectionConfig::parse("tcp://127.0.0.1:25325?password=secret");
        $this->assertEquals("tcp://127.0.0.1:25325", $connectionConfig->getUri());
        $this->assertTrue($connectionConfig->hasPassword());
        $this->assertEquals("secret", $connectionConfig->getPassword());
        $this->assertEquals(0, $connectionConfig->getDatabase());
        $this->assertEquals(5000, $connectionConfig->getTimeout());

        $connectionConfig = ConnectionConfig::parse("tcp://127.0.0.1:25325?password=0");
        $this->assertEquals("tcp://127.0.0.1:25325", $connectionConfig->getUri());
        $this->assertTrue($connectionConfig->hasPassword());
        $this->assertEquals("0", $connectionConfig->getPassword());
        $this->assertEquals(0, $connectionConfig->getDatabase());
        $this->assertEquals(5000, $connectionConfig->getTimeout());
    }

    /**
     * @test
     */
    public function parseTcpUriWithDatabaseAndPassword()
    {
        $connectionConfig = ConnectionConfig::parse("tcp://127.0.0.1:25325?database=1&password=secret");
        $this->assertEquals("tcp://127.0.0.1:25325", $connectionConfig->getUri());
        $this->assertTrue($connectionConfig->hasPassword());
        $this->assertEquals("secret", $connectionConfig->getPassword());
        $this->assertEquals(1, $connectionConfig->getDatabase());
        $this->assertEquals(5000, $connectionConfig->getTimeout());
    }

    /**
     * @test
     */
    public function parseUnixUri()
    {
        $connectionConfig = ConnectionConfig::parse("unix:/run/redis.sock");
        $this->assertEquals("unix:/run/redis.sock", $connectionConfig->getUri());
        $this->assertFalse($connectionConfig->hasPassword());
        $this->assertEquals(0, $connectionConfig->getDatabase());
        $this->assertEquals(5000, $connectionConfig->getTimeout());
    }

    /**
     * @test
     */
    public function parseUnixUriWithTimeout()
    {
        $connectionConfig = ConnectionConfig::parse("unix:/run/redis.sock?timeout=1000");
        $this->assertEquals("unix:/run/redis.sock", $connectionConfig->getUri());
        $this->assertFalse($connectionConfig->hasPassword());
        $this->assertEquals(0, $connectionConfig->getDatabase());
        $this->assertEquals(1000, $connectionConfig->getTimeout());
    }

    /**
     * @test
     */
    public function parseUnixUriWithDatabase()
    {
        $connectionConfig = ConnectionConfig::parse("unix:/run/redis.sock?database=1");
        $this->assertEquals("unix:/run/redis.sock", $connectionConfig->getUri());
        $this->assertFalse($connectionConfig->hasPassword());
        $this->assertEquals(1, $connectionConfig->getDatabase());
        $this->assertEquals(5000, $connectionConfig->getTimeout());
    }

    /**
     * @test
     */
    public function parseUnixUriWithPassword()
    {
        $connectionConfig = ConnectionConfig::parse("unix:/run/redis.sock?password=secret");
        $this->assertEquals("unix:/run/redis.sock", $connectionConfig->getUri());
        $this->assertTrue($connectionConfig->hasPassword());
        $this->assertEquals("secret", $connectionConfig->getPassword());
        $this->assertEquals(0, $connectionConfig->getDatabase());
        $this->assertEquals(5000, $connectionConfig->getTimeout());

        $connectionConfig = ConnectionConfig::parse("unix:///run/redis.sock?password=0");
        $this->assertEquals("unix:/run/redis.sock", $connectionConfig->getUri());
        $this->assertTrue($connectionConfig->hasPassword());
        $this->assertEquals("0", $connectionConfig->getPassword());
        $this->assertEquals(0, $connectionConfig->getDatabase());
        $this->assertEquals(5000, $connectionConfig->getTimeout());
    }

    /**
     * @test
     */
    public function parseUnixUriWithDatabaseAndPassword()
    {
        $connectionConfig = ConnectionConfig::parse("unix:/run/redis.sock?database=1&password=secret");
        $this->assertEquals("unix:/run/redis.sock", $connectionConfig->getUri());
        $this->assertTrue($connectionConfig->hasPassword());
        $this->assertEquals("secret", $connectionConfig->getPassword());
        $this->assertEquals(1, $connectionConfig->getDatabase());
        $this->assertEquals(5000, $connectionConfig->getTimeout());
    }

    /**
     * @test
     */
    public function parseRedisUri()
    {
        $connectionConfig = ConnectionConfig::parse("redis://127.0.0.1:25325");
        $this->assertEquals("tcp://127.0.0.1:25325", $connectionConfig->getUri());
        $this->assertFalse($connectionConfig->hasPassword());
        $this->assertEquals(0, $connectionConfig->getDatabase());
        $this->assertEquals(5000, $connectionConfig->getTimeout());

        $connectionConfig = ConnectionConfig::parse("redis://127.0.0.1");
        $this->assertEquals("tcp://127.0.0.1:6379", $connectionConfig->getUri());
        $this->assertFalse($connectionConfig->hasPassword());
        $this->assertEquals(0, $connectionConfig->getDatabase());
        $this->assertEquals(5000, $connectionConfig->getTimeout());

        $connectionConfig = ConnectionConfig::parse("redis://");
        $this->assertEquals("tcp://localhost:6379", $connectionConfig->getUri());
        $this->assertFalse($connectionConfig->hasPassword());
        $this->assertEquals(0, $connectionConfig->getDatabase());
        $this->assertEquals(5000, $connectionConfig->getTimeout());
    }

    /**
     * @test
     */
    public function parseRedisUriWithTimeout()
    {
        $connectionConfig = ConnectionConfig::parse("redis://127.0.0.1:25325/1?timeout=1000");
        $this->assertEquals("tcp://127.0.0.1:25325", $connectionConfig->getUri());
        $this->assertFalse($connectionConfig->hasPassword());
        $this->assertEquals(1, $connectionConfig->getDatabase());
        $this->assertEquals(1000, $connectionConfig->getTimeout());
    }

    /**
     * @test
     */
    public function parseRedisUriWithDatabase()
    {
        $connectionConfig = ConnectionConfig::parse("redis://127.0.0.1:25325/1");
        $this->assertEquals("tcp://127.0.0.1:25325", $connectionConfig->getUri());
        $this->assertFalse($connectionConfig->hasPassword());
        $this->assertEquals(1, $connectionConfig->getDatabase());
        $this->assertEquals(5000, $connectionConfig->getTimeout());
    }

    /**
     * @test
     * @expectedException \Amp\Redis\ConnectionConfigException
     */
    public function failParseRedisUriWithDatabasePathAndQuery()
    {
        ConnectionConfig::parse("redis://127.0.0.1:25325/1?db=2");
    }

    /**
     * @test
     */
    public function parseRedisUriWithPassword()
    {
        $connectionConfig = ConnectionConfig::parse("redis://:secret@127.0.0.1:25325");
        $this->assertEquals("tcp://127.0.0.1:25325", $connectionConfig->getUri());
        $this->assertTrue($connectionConfig->hasPassword());
        $this->assertEquals("secret", $connectionConfig->getPassword());
        $this->assertEquals(0, $connectionConfig->getDatabase());
        $this->assertEquals(5000, $connectionConfig->getTimeout());

        $connectionConfig = ConnectionConfig::parse("redis://:0@127.0.0.1:25325");
        $this->assertEquals("tcp://127.0.0.1:25325", $connectionConfig->getUri());
        $this->assertTrue($connectionConfig->hasPassword());
        $this->assertEquals("0", $connectionConfig->getPassword());
        $this->assertEquals(0, $connectionConfig->getDatabase());
        $this->assertEquals(5000, $connectionConfig->getTimeout());
    }

    /**
     * @test
     * @expectedException \Amp\Redis\ConnectionConfigException
     */
    public function parseRedisUriWithPasswordInUserInfoAndQuery()
    {
        ConnectionConfig::parse("redis://:secret@127.0.0.1:25325?password=secret");
    }

    /**
     * @test
     */
    public function parseRedisUriWithDatabaseAndPassword()
    {
        $connectionConfig = ConnectionConfig::parse("redis://:secret@127.0.0.1:25325/1");
        $this->assertEquals("tcp://127.0.0.1:25325", $connectionConfig->getUri());
        $this->assertTrue($connectionConfig->hasPassword());
        $this->assertEquals("secret", $connectionConfig->getPassword());
        $this->assertEquals(1, $connectionConfig->getDatabase());
        $this->assertEquals(5000, $connectionConfig->getTimeout());
    }
}
