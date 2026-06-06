<?php

namespace Cekta\Queue\Postgres;

use DateMalformedStringException;
use DateTimeImmutable;
use DateTimeInterface;
use InvalidArgumentException;
use PDO;
use Psr\Clock\ClockInterface;
use RuntimeException;

class Consumer
{
    public function __construct(
        private PDO $pdo,
        private ClockInterface $clock,
    ) {
    }

    public function getNext(): ?TaskDTO
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

    public function finish(string $uuid, bool $result): void
    {
        $status = $result ? Status::SUCCESS->value : Status::FAIL->value;
        $this->pdo->prepare("UPDATE tasks SET status = :status, finished_at = :finished_at WHERE uuid = :uuid")
            ->execute([
                'uuid' => $uuid,
                'status' => $status,
                'finished_at' => $this->clock->now()->format(DateTimeInterface::RFC3339),
            ]);
    }

    private function getNextUUID(): ?string
    {
        $stm = $this->pdo->query(
            "DELETE FROM queue_default
                         WHERE uuid = (
                             SELECT uuid FROM queue_default
                             ORDER BY uuid
                             LIMIT 1
                             FOR UPDATE SKIP LOCKED
                         )
                         RETURNING uuid"
        );
        if ($stm === false) {
            return null;
        }
        $row = $stm->fetch();
        if (
            !is_array($row)
            || !is_string($row['uuid'])
        ) {
            return null;
        }
        return $row['uuid'];
    }

    private function getTask(string $uuid): TaskDTO
    {
        $stm = $this->pdo->prepare(
            "UPDATE tasks 
                        SET status = :status, started_at = :started_at
                        WHERE uuid = :uuid 
                        RETURNING *"
        );
        $stm->execute([
            'uuid' => $uuid,
            'status' => Status::PROCESSING->value,
            'started_at' => $this->clock->now()->format(DateTimeInterface::RFC3339),
        ]);
        $row = $stm->fetch(PDO::FETCH_ASSOC);
        if (
            !is_array($row)
            || !is_string($row['uuid'])
            || !is_string($row['fqcn'])
            || !is_string($row['handler'])
            || !is_string($row['payload'])
            || !is_string($row['status'])
            || !is_string($row['created_at'])
            || !(is_string($row['started_at']) || null === $row['started_at'])
            || !(is_string($row['finished_at']) || null === $row['finished_at'])
        ) {
            if ($this->pdo->inTransaction()) {
                $this->pdo->rollBack();
            }
            throw new InvalidArgumentException("Task not found");
        }
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
            throw new RuntimeException($e->getMessage());
        }
    }
}
