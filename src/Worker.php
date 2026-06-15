<?php

declare(strict_types=1);

namespace Cekta\Queue\Postgres;

use Psr\Log\LoggerInterface;
use Throwable;

class Worker
{
    private bool $shouldStop = false;
    public function __construct(
        private Consumer $consumer,
        private LoggerInterface $logger,
        private int $sleep = 1,
    ) {
    }

    public function work(): int
    {
        pcntl_async_signals(true);
        pcntl_signal(SIGTERM, [$this, 'handleSignal']);
        pcntl_signal(SIGINT, [$this, 'handleSignal']);
        while (!$this->shouldStop) {
            try {
                $this->consumer->consume();
            } catch (Throwable $e) {
                $this->logger->error($e);
                sleep($this->sleep);
                continue;
            }
        }
        return 0;
    }

    public function handleSignal(int $signal): void
    {
        $this->shouldStop = true;
    }
}
