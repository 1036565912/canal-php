<?php

namespace Chenlin\CanalPhp\DealInterface;

interface DealInterface
{

    /**
     * 记录更新相关记录
     * @param array $record 更新的记录信息[包含所有的字段]
     * @return bool
     */
    public function update(array $record): bool;

    /**
     * 删除相关记录
     * @param array $record  example: ['id' => ['value' => 1, 'update' => true]]
     * @return bool
     */
    public function delete(array $record): bool;

    /**
     * 添加数据
     * @param array $record
     * @return bool
     */
    public function insert(array $record): bool;
}