<?php

declare(strict_types=1);

namespace Cekta\Queue\Postgres;

use Cekta\Queue\Status;
use Cekta\Queue\Task;
use Cekta\Queue\TaskDTO;
use DateMalformedStringException;
use DateTimeImmutable;
use DateTimeInterface;
use JsonSerializable;
use PDO;
use Psr\Clock\ClockInterface;
use RuntimeException;

class TaskRepository
{
    public function __construct(
        private PDO $pdo,
        private ClockInterface $clock,
        private string $table = "tasks",
        private string $queueName = "queue_default",
    ) {
    }

    public function findByUuid(string $uuid): ?Task
    {
        $sth = $this->pdo->prepare("SELECT * FROM tasks WHERE uuid = :uuid");
        $sth->execute(['uuid' => $uuid]);
        $row = $sth->fetch(PDO::FETCH_ASSOC);
        if ($row === false) {
            return null;
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

    public function push(string $uuid, JsonSerializable $payload, string $handler): void
    {
        $this->pdo->beginTransaction();

        $query = sprintf(
            '
INSERT INTO "%s" (
    "uuid",
    "queue_name",
    "handler",
    "fqcn",
    "payload",
    "created_at", 
    "status"
) VALUES (
    :uuid, 
    :queue_name,
    :handler,
    :fqcn,
    :payload,
    :created_at,
    :status
)',
            $this->table
        );
        $sth = $this->pdo->prepare($query);
        $sth->execute([
            'uuid' => $uuid,
            'queue_name' => $this->queueName,
            'fqcn' => $payload::class,
            'payload' => json_encode($payload),
            'created_at' => $this->clock->now()->format(DateTimeInterface::RFC3339),
            'status' => Status::PENDING->value,
            'handler' => $handler
        ]);

        $sth = $this->pdo->prepare("INSERT INTO {$this->queueName}(uuid) VALUES (:uuid)");
        $sth->execute([
            'uuid' => $uuid,
        ]);

        $this->pdo->commit();
    }

    public function storeResult(string $uuid, bool $result): void
    {
        $status = $result ? Status::SUCCESS->value : Status::FAIL->value;
        $this->pdo->prepare("UPDATE tasks SET status = :status, finished_at = :finished_at WHERE uuid = :uuid")
            ->execute([
                'uuid' => $uuid,
                'status' => $status,
                'finished_at' => $this->clock->now()->format(DateTimeInterface::RFC3339),
            ]);
    }

    public function findNext(): ?Task
    {
        $this->pdo->beginTransaction();
        $uuid = $this->findNextUUID();
        if ($uuid === null) {
            $this->pdo->commit();
            return null;
        }
        $task = $this->findByUuid($uuid);
        $this->pdo->commit();
        return $task;
    }

    private function findNextUUID(): ?string
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
                        SET status = :status, 
                            started_at = :started_at,
                            started_hostname = :started_hostname,
                            started_pid = :started_pid
                        WHERE uuid = :uuid"
        )->execute([
            'uuid' => $row['uuid'],
            'status' => Status::PROCESSING->value,
            'started_at' => $this->clock->now()->format(DateTimeInterface::RFC3339),
            'started_hostname' => gethostname(),
            'started_pid' => getmypid(),
        ]);
        return $row['uuid'];
    }
}
