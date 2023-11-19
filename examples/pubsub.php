<?php declare(strict_types=1);

require __DIR__ . '/../vendor/autoload.php';

use Amp\Redis\RedisSubscriber;
use function Amp\async;
use function Amp\delay;
use function Amp\Redis\createRedisClient;
use function Amp\Redis\createRedisConnector;

$client = createRedisClient('redis://');
$subscriber = new RedisSubscriber(createRedisConnector('redis://'));

$subscription = $subscriber->subscribe('amphp');

async(function () use ($client) {
    delay(3); // wait for subscription to be active

    $client->publish('amphp', 'New release is out!');
});

foreach ($subscription as $message) {
    var_dump($message);

    break;
}
