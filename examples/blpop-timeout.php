<?php /** @noinspection ForgottenDebugOutputInspection */

require __DIR__ . '/../vendor/autoload.php';

$config = Amp\Redis\Config::fromUri('tcp://localhost:6379');
$client = new Amp\Redis\Redis(new Amp\Redis\RemoteExecutor($config));

$client->delete('foobar-list');

Amp\Loop::unreference(Amp\Loop::repeat(1000, static function (): void {
    print 'Waiting for blpop…' . PHP_EOL;
}));

$value = $client->getList('foobar-list')->popHeadBlocking(5);

\var_dump($value);
