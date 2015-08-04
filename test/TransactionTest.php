<?php

namespace Amp\Redis;

use function Amp\driver;
use function Amp\reactor;
use function Amp\run;

class TransactionTest extends RedisTest {
    /**
     * @test
     */
    function success () {
        reactor(driver())->run(function () {
            $_1 = new Client("tcp://127.0.0.1:25325", []);
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
        reactor(driver())->run(function () {
            $_1 = new Client("tcp://127.0.0.1:25325", []);
            $_2 = new Client("tcp://127.0.0.1:25325", []);

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
