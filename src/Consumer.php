<?php

namespace Cekta\Queue\Postgres;

use DateMalformedStringException;
use DateTimeImmutable;
use DateTimeInterface;
use PDO;
use Psr\Clock\ClockInterface;
use RuntimeException;

readonly class Consumer
{
    public function __construct(
        private PDO $pdo,
        private HandlerProvider $handlerProvider,
        private ClockInterface $clock,
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
        return $this->getTask($task->uuid);
    }

    private function getNext(): ?TaskDTO
    {
        $this->pdo->beginTransaction();
        $uuid = $this->getNextUUID();
        if ($uuid === null) {
            $this->pdo->commit();
            return null;
        }
        $task = $this->getTask($uuid);
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
        if (
            !is_array($row)
            || !is_string($row['uuid'])
        ) {
            return null;
        }
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

    private function getTask(string $uuid): TaskDTO
    {
        $sth = $this->pdo->prepare("SELECT * FROM tasks WHERE uuid = :uuid");
        $sth->execute(['uuid' => $uuid]);
        $row = $sth->fetch(PDO::FETCH_ASSOC);
        if ($row === false) {
            throw new RuntimeException("Task not found");
        }
        /** @var array{
         * uuid: string,
         * queue_name: string,
         * handler: string,
         * fqcn: string,
         * payload: string,
         * created_at: string,
         * started_at: ?string,
         * finished_at: ?string,
         * status: string
         * } $row
         */
        try {
            return new TaskDTO(
                $row['uuid'],
                $row['fqcn'],
                $row['handler'],
                json_decode($row['payload'], true),
                Status::from($row['status']),
                new DateTimeImmutable($row['created_at']),
                is_string($row['started_at']) ? new DateTimeImmutable($row['started_at']) : null,
                is_string($row['finished_at']) ? new DateTimeImmutable($row['finished_at']) : null,
            );
        } catch (DateMalformedStringException $e) {
            throw new RuntimeException(message: $e->getMessage(), previous: $e);
        }
    }
}
