<?php

require __DIR__ . "/../vendor/autoload.php";

use Amp\Redis\RespParser;

$bench = new Hoa\Bench\Bench();
$parser = new RespParser(function ($data)
{
    // ignore for now
});

$bench->parse->start();
for ($i = 0; $i < 150000; $i ++) {
    $parser->append("*2\r\n$5\r\nHello\r\n:123456789\r\n");
}
$bench->parse->stop();
echo $bench;