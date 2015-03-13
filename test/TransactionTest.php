<?php

namespace Amp\Redis;

use Amp\NativeReactor;
use function Amp\run;

class TransactionTest extends \PHPUnit_Framework_TestCase {
    static function setUpBeforeClass () {
        print `redis-server --daemonize yes --port 25325 --timeout 3 --pidfile /tmp/amp-redis.pid`;
        sleep(2);
    }

    static function tearDownAfterClass () {
        $pid = @file_get_contents("/tmp/amp-redis.pid");
        @unlink("/tmp/amp-redis.pid");

        if (!empty($pid)) {
            print `kill $pid`;
            sleep(2);
        }
    }

    /**
     * @test
     */
    function success () {
        (new NativeReactor())->run(function ($reactor) {
            $_1 = new Client("tcp://127.0.0.1:25325", null, $reactor);
            yield $_1->set("key", "1");

            $transaction = $_1->transaction();
            $transaction->watch("key");
            $cnt = (yield $transaction->get("key"));
            $cnt = $cnt + 1;
            $transaction->multi();
            $transaction->set("key", $cnt);
            $transaction->exec();

            $this->assertEquals("2", (yield $_1->get("key")));
        });
    }

    /**
     * @test
     * @expectedException \Amp\Redis\RedisException
     */
    function failure () {
        (new NativeReactor())->run(function ($reactor) {
            $_1 = new Client("tcp://127.0.0.1:25325", null, $reactor);
            $_2 = new Client("tcp://127.0.0.1:25325", null, $reactor);

            yield $_1->set("key", "1");

            $transaction = $_1->transaction();
            $transaction->watch("key");
            $cnt = (yield $transaction->get("key"));
            $cnt = $cnt + 1;
            $transaction->multi();
            $transaction->set("key", $cnt);

            yield $_2->set("key", "3");
            yield $transaction->exec();
        });
    }
}
