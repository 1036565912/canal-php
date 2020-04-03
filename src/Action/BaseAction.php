<?php
declare(strict_types=1);

namespace Chenlin\CanalPhp\Action;

use Com\Alibaba\Otter\Canal\Protocol\Column;
use Chenlin\CanalPhp\DealInterface\DealInterface;
use Com\Alibaba\Otter\Canal\Protocol\Header;
use Com\Alibaba\Otter\Canal\Protocol\EventType;

abstract class BaseAction
{

    /** @var array $entrys [包含的都是Entry] */
    protected  $entrys = [];

    /** @var array 事件映射数组 */
    protected  $event_arr = [
        EventType::INSERT => '添加',
        EventType::UPDATE => '更新',
        EventType::DELETE => '删除'
    ];

    /**
     * 构造函数
     * DealProvinceAction constructor.
     * @param array  $entry [数据类型为: Entry]
     */
    public function __construct(array $entry)
    {
        $this->entrys = $entry;
    }


    /**
     * 自定义同步数据的业务逻辑
     * @param DealInterface $deal
     * @param string $record_log 常规日志记录文件
     * @param string $error_log 错误日志记录文件
     * @return mixed
     */
    abstract public function async(DealInterface $deal, string $record_log, string $error_log);

    /**
     * 转化记录数据格式
     * @param Column $columns
     * @return array
     */
    public function dealColumn($columns): array
    {

        $field_list = [];
        /** @var Column $column */
        foreach ($columns as $column) {
            $field_list[$column->getName()] = ['value' => $column->getValue(), 'update' => $column->getUpdated()];
        }
        return $field_list;
    }

    /**
     * 日志记录接口[用于记录数据]
     * @param string $log_path 日志文件路径
     * @param int $eventType 当前时间类型[insert,update,delete]
     * @param array $row_data 原始数据[由self::dealColumn 处理得到的数据]
     * @param Header $header Header头信息 可以获取对应的数据库,表
     * @return bool
     */
    abstract  public function recordLog(string $log_path, int $eventType, array $row_data, Header $header): bool;

    /**
     * 同步数据失败重试机制
     * @param int $eventType
     * @param array $row_data
     * @param Header $header
     * @param string $error_log 错误日志文件
     * @param DealInterface $deal
     * @param  int $try_num 重试次数
     * @return void
     */
    abstract  public function asyncRetry(int $eventType, array $row_data, Header $header, string $error_log, DealInterface $deal, int $try_num = 3): void;
}