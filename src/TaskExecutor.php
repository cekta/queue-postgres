<?php

declare(strict_types=1);

namespace Cekta\Queue\Postgres;

use Cekta\Queue\Task;

class TaskExecutor
{
    public function __construct(
        private TaskRepository $taskRepository,
        private HandlerProvider $handlerProvider,
    ) {
    }

    public function execute(Task $task): bool
    {
        try {
            $result = $this->handlerProvider->getHandler($task->getHandler())->handle($task);
        } catch (\Throwable $exception) {
            $result = false;
        }
        $this->taskRepository->storeResult($task->getUuid(), $result);
        return $result;
    }
}
