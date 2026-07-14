<?php

declare(strict_types=1);

namespace Cekta\Queue\Postgres;

use Cekta\Queue\Status;
use Psr\Log\LoggerInterface;

class Consumer implements \Cekta\Queue\Consumer
{
    private bool $shouldStop = false;

    public function __construct(
        private TaskRepository $taskRepository,
        private HandlerProvider $handlerProvider,
        private LoggerInterface $logger,
        private int $usleepAfterNotihing = 1000 * 300,
    ) {
    }

    public function stop(): void
    {
        $this->shouldStop = true;
    }

    public function run(): int
    {
        while (!$this->shouldStop) {
            $this->runOnce();
        }
        $this->logger->info('worker stopped');
        return 0;
    }

    public function runOnce(): void
    {
        $task = $this->taskRepository->findNext();
        if (null === $task) {
            $this->logger->debug('nothing todo');
            usleep($this->usleepAfterNotihing);
            return;
        }
        try {
            $result = $this->handlerProvider->getHandler($task->getHandler())->handle($task);
        } catch (\Throwable $exception) {
            $this->logger->emergency($exception);
            exit(1);
        }
        $this->taskRepository->updateStatus(
            $task->getUuid(),
            $result ? Status::SUCCESS : Status::FAIL
        );
    }

    public function failExpiredTasks(int $expiredSecond): int
    {
        foreach ($this->taskRepository->findExpired($expiredSecond) as $task) {
            $this->taskRepository->updateStatus($task->getUuid(), Status::FAIL);
        }
        return 0;
    }
}
