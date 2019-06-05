<?php

require __DIR__ . "/../vendor/autoload.php";

Amp\Loop::run(function () {
    $client = new Amp\Redis\Client("tcp://localhost:6379");

    yield $client->del("foobar-list");

    Amp\Loop::unreference(Amp\Loop::repeat(1000, function () {
        print "Waiting for blpop…" . PHP_EOL;
    }, 1000));

    $value = yield $client->blpop("foobar-list", 5);
    \var_dump($value);
});
