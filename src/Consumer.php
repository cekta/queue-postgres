<?php

namespace Cekta\Queue\Postgres;

use DateTimeInterface;
use PDO;
use Psr\Clock\ClockInterface;

readonly class Consumer
{
    public function __construct(
        private PDO $pdo,
        private HandlerProvider $handlerProvider,
        private ClockInterface $clock,
        private TaskProvider $taskProvider,
    ) {
    }

    public function consume(): ?TaskDTO
    {
        $task = $this->getNext();
        if ($task === null) {
            return null;
        }
        $handler = $this->handlerProvider->getHandler($task->handler);
        $status = $handler->handle($task) ? Status::SUCCESS->value : Status::FAIL->value;
        $this->pdo->prepare("UPDATE tasks SET status = :status, finished_at = :finished_at WHERE uuid = :uuid")
            ->execute([
                'uuid' => $task->uuid,
                'status' => $status,
                'finished_at' => $this->clock->now()->format(DateTimeInterface::RFC3339),
            ]);
        return $this->taskProvider->get($task->uuid);
    }

    private function getNext(): ?TaskDTO
    {
        $this->pdo->beginTransaction();
        $uuid = $this->getNextUUID();
        if ($uuid === null) {
            $this->pdo->commit();
            return null;
        }
        $task = $this->taskProvider->get($uuid);
        $this->pdo->commit();
        return $task;
    }

    private function getNextUUID(): ?string
    {
        $stm = $this->pdo->prepare(
            "DELETE FROM queue_default
                         WHERE uuid = (
                             SELECT uuid FROM queue_default
                             ORDER BY uuid
                             LIMIT 1
                             FOR UPDATE SKIP LOCKED
                         )
                         RETURNING uuid"
        );
        $stm->execute();
        $row = $stm->fetch();
        if (!is_array($row)) {
            return null;
        }
        /** @var array{uuid: string} $row */
        $this->pdo->prepare(
            "UPDATE tasks 
                        SET status = :status, started_at = :started_at
                        WHERE uuid = :uuid"
        )->execute([
            'uuid' => $row['uuid'],
            'status' => Status::PROCESSING->value,
            'started_at' => $this->clock->now()->format(DateTimeInterface::RFC3339),
        ]);
        return $row['uuid'];
    }
}
