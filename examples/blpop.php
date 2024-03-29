<?php declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use function Amp\async;
use function Amp\Redis\createRedisClient;

$future = async(function (): void {
    $popClient = createRedisClient('redis://');

    try {
        $value = $popClient->getList('foobar-list')->popHeadBlocking();
        print 'Value: ' . var_export($value, true) . PHP_EOL;
    } catch (\Throwable $error) {
        print 'Error: ' . $error->getMessage() . PHP_EOL;
    }
});

$pushClient = createRedisClient('redis://');

print 'Pushing value…' . PHP_EOL;
$pushClient->getList('foobar-list')->pushHead('42');
print 'Value pushed.' . PHP_EOL;

$future->await();
