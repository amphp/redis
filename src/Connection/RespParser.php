<?php

namespace Amp\Redis\Connection;

use Amp\Pipeline\Queue;
use Amp\Redis\ParserException;
use Amp\Redis\QueryException;

/**
 * @psalm-suppress UnsupportedReferenceUsage
 */
final class RespParser
{
    public const CRLF = "\r\n";
    public const TYPE_SIMPLE_STRING = '+';
    public const TYPE_ERROR = '-';
    public const TYPE_ARRAY = '*';
    public const TYPE_BULK_STRING = '$';
    public const TYPE_INTEGER = ':';

    private string $buffer = '';
    private ?array $currentResponse = null;
    private array $arrayStack = [];
    private int $currentSize = 0;
    private array $arraySizes = [];

    public function __construct(
        private readonly Queue $queue,
    ) {
    }

    public function append(string $data): void
    {
        $this->buffer .= $data;

        while (\strlen($this->buffer)) {
            $type = $this->buffer[0];
            $pos = \strpos($this->buffer, self::CRLF);
            if ($pos === false) {
                return;
            }

            switch ($type) {
                case self::TYPE_SIMPLE_STRING:
                case self::TYPE_INTEGER:
                case self::TYPE_ARRAY:
                case self::TYPE_ERROR:
                    $payload = \substr($this->buffer, 1, $pos - 1);
                    $remove = $pos + 2;
                    break;

                case self::TYPE_BULK_STRING:
                    $length = (int) \substr($this->buffer, 1, $pos);

                    if ($length === -1) {
                        $payload = null;
                        $remove = $pos + 2;
                        break;
                    }

                    if (\strlen($this->buffer) < $pos + $length + 4) {
                        return; // Entire payload not received.
                    }

                    $payload = \substr($this->buffer, $pos + 2, $length);
                    $remove = $pos + $length + 4;
                    break;

                default:
                    throw new ParserException('Unknown resp data type: ' . $type);
            }

            $this->buffer = \substr($this->buffer, $remove);

            switch ($type) {
                case self::TYPE_INTEGER:
                case self::TYPE_ARRAY:
                    $payload = (int) $payload;
                    break;

                case self::TYPE_ERROR:
                    $this->queue->push(new RespError(new QueryException($payload ?? 'Unknown error')));
                    continue 2;

                default:
                    break;
            }

            if ($this->currentResponse !== null) { // extend array response
                if ($type === self::TYPE_ARRAY) {
                    \assert(\is_int($payload));
                    if ($payload >= 0) {
                        $this->arraySizes[] = $this->currentSize;
                        $this->arrayStack[] = &$this->currentResponse;
                        $this->currentSize = $payload + 1;
                        $this->currentResponse[] = [];
                        $this->currentResponse = &$this->currentResponse[\sizeof($this->currentResponse) - 1];
                    } else {
                        $this->currentResponse[] = null;
                    }
                } else {
                    $this->currentResponse[] = $payload;
                }

                while (--$this->currentSize === 0) {
                    if (\count($this->arrayStack) === 0) {
                        $this->queue->push(new RespValue($this->currentResponse));
                        $this->currentResponse = null;
                        break;
                    }

                    // index doesn't start at 0 :(
                    \end($this->arrayStack);
                    $key = \key($this->arrayStack);
                    $this->currentResponse = &$this->arrayStack[$key];
                    $this->currentSize = \array_pop($this->arraySizes);
                    unset($this->arrayStack[$key]);
                }
            } elseif ($type === self::TYPE_ARRAY) { // start new array response
                \assert(\is_int($payload));
                if ($payload > 0) {
                    $this->currentSize = $payload;
                    $this->arrayStack = $this->arraySizes = $this->currentResponse = [];
                } elseif ($payload === 0) {
                    $this->queue->push(new RespValue([]));
                } else {
                    $this->queue->push(new RespValue(null));
                }
            } else { // single data type response
                $this->queue->push(new RespValue($payload));
            }
        }
    }
}
