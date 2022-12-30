<?php

namespace Amp\Redis\Connection;

use Amp\Parser\Parser;
use Amp\Pipeline\Queue;
use Amp\Redis\ParserException;
use Amp\Redis\QueryException;

final class RespParser extends Parser
{
    private const CRLF = "\r\n";

    private const TYPE_SIMPLE_STRING = '+';
    private const TYPE_ERROR = '-';
    private const TYPE_ARRAY = '*';
    private const TYPE_BULK_STRING = '$';
    private const TYPE_INTEGER = ':';

    public function __construct(Queue $queue)
    {
        parent::__construct(self::parser($queue));
    }

    public static function parser(Queue $queue): \Generator
    {
        while (true) {
            try {
                $queue->push(new RespValue(yield from self::consume()));
            } catch (QueryException $e) {
                $queue->push(new RespError($e));
            }
        }
    }

    private static function consume(): \Generator
    {
        $type = yield 1;
        $payload = yield self::CRLF;

        switch ($type) {
            case self::TYPE_SIMPLE_STRING:
                return $payload;

            case self::TYPE_INTEGER:
                return (int) $payload;

            case self::TYPE_BULK_STRING:
                $length = (int) $payload;

                if ($length === -1) {
                    return null;
                }

                $payload = match ($length) {
                    0 => '',
                    default => yield $length,
                };

                yield self::CRLF;

                return $payload;

            case self::TYPE_ARRAY:
                $count = (int) $payload;

                if ($count === -1) {
                    return null;
                }

                $payload = [];
                for ($i = 0; $i < $count; $i++) {
                    $payload[] = yield from self::consume();
                }

                return $payload;

            case self::TYPE_ERROR:
                throw new QueryException($payload);

            default:
                throw new ParserException('Unknown resp data type: ' . $type);
        }
    }
}
