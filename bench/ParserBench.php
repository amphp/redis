<?php

require __DIR__ . "/../lib/RespParser.php";

use Amp\Redis\RespParser;

$parser = new RespParser(function ($data)
{
    // ignore for now
});
for ($i = 0; $i < 150000; $i ++) {
    $parser->append("*2\r\n$5\r\nHello\r\n:123456789\r\n");
}
echo "done\n";