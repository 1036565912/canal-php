<?php
declare(strict_types=1);

namespace Chenlin\CanalPhp\Example;

use Chenlin\CanalPhp\Action\BaseAction;
use Com\Alibaba\Otter\Canal\Protocol\Entry;
use Com\Alibaba\Otter\Canal\Protocol\EntryType;
use Com\Alibaba\Otter\Canal\Protocol\EventType;
use Com\Alibaba\Otter\Canal\Protocol\Header;
use Com\Alibaba\Otter\Canal\Protocol\RowChange;
use Com\Alibaba\Otter\Canal\Protocol\RowData;
use Chenlin\CanalPhp\DealInterface\DealInterface;
use Monolog\Formatter\LineFormatter;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

class DemoAction extends BaseAction
{
    /**
     * @param DealInterface $deal
     * @throws \Exception
     */
    public function async(DealInterface $deal, string $record_log, string $error_log)
    {
        /** @var Entry $entry */
        foreach ($this->entrys as $entry) {
            if (in_array($entry->getEntryType(), [EntryType::TRANSACTIONBEGIN,EntryType::TRANSACTIONEND])) {
                //如果是事务 则跳过
                continue;
            }

            //如果不是事务 则继续执行同步逻辑
            $rowChange = new RowChange();
            $rowChange->mergeFromString($entry->getStoreValue());
            $evenType = $rowChange->getEventType();
            $header = $entry->getHeader();  //header中保存了 数据库 表  以及 bin-log相关信息

            /** @var RowData $rowData */
            foreach ($rowChange->getRowDatas() as $rowData) {
                $result = false;
                $row_data = []; //保存原始数据
                //进行事件判断
                switch ($evenType) {
                    case EventType::DELETE :
                        echo '删除数据同步'.PHP_EOL;
                        $row_data = $this->dealColumn($rowData->getBeforeColumns());
                        $result = $deal->delete($row_data);
                        break;
                    case EventType::INSERT :
                        echo '新增数据同步'.PHP_EOL;
                        $row_data = $this->dealColumn($rowData->getAfterColumns());
                        $result = $deal->insert($row_data);
                        break;
                    case EventType::UPDATE :
                        //默认则是修改
                        echo '更新数据同步'.PHP_EOL;
                        $row_data = $this->dealColumn($rowData->getAfterColumns());
                        $result = $deal->update($row_data);
                }
                //@tip: 这里存在其他的一些操作事件 要注意防范
                //如果同步失败 则需要进行重试
                if (!$result && in_array($evenType, $this->event_arr)) {
                    //默认重试3次
                    $this->asyncRetry($evenType, $row_data, $header, $error_log, $deal);
                }

                //如果同步成功  则进行日志记录
                if (in_array($evenType, $this->event_arr)) {
                    $this->recordLog($record_log, $evenType, $row_data, $header);
                }
            }
        }
    }

    /**
     * 简单的一个monolog日志记录方法
     * @param string $log_path
     * @param int $eventType
     * @param array $row_data
     * @param Header $header
     * @return bool
     */
    public function recordLog(string $log_path, int $eventType, array $row_data, Header $header): bool
    {
        // TODO: Implement recordLog() method.
        //定制日志格式
        $info = [
            'database' => $header->getSchemaName(),
            'table' => $header->getTableName(),
            'bin-log' => $header->getLogfileName(),
            'offset' => $header->getLogfileOffset(),
            'eventType' => $this->event_arr[$eventType],
            'rowData' => $row_data,
            'describe' => 'async data success!'
        ];

        $log = new Logger('async_record');
        if (!file_exists($log_path)) {
            throw new \Exception('日志记录文件不存在');
        }
        $dateFormat = "Y-m-d H:i:s";
        $output = "[%datetime%] %channel%.%level_name%: %message% %context% %extra%\n";
        $format = new LineFormatter($output, $dateFormat);
        //设置日志记录时间格式
        $stream = new StreamHandler($log_path, Logger::DEBUG);
        $stream->setFormatter($format);

        $log->pushHandler($stream);
        $log->info(json_encode($info, JSON_UNESCAPED_UNICODE));
        return true;
    }


    /**
     * 简单的同步失败重试方法
     * @param int $eventType
     * @param array $row_data
     * @param Header $header
     * @param string $error_log
     * @param DealInterface $deal
     * @return void
     */
    public function asyncRetry(int $eventType, array $row_data, Header $header, string $error_log, DealInterface $deal, int $try_num = 3): void
    {
        // TODO: Implement asyncRetry() method.
        //第一步判断是否满足符合事件条件
        if (!in_array($eventType, $this->event_arr)) {
            return ;
        }

        //满足  则记录相关的错误信息
        $log = new Logger('async_fail_log');
        //判断文件是否存在
        if (!file_exists($error_log)) {
            throw new \Exception('错误日志文件不存在');
        }


        $dateFormat = "Y-m-d H:i:s";
        $output = "[%datetime%] %channel%.%level_name%: %message% %context% %extra%\n";
        $format = new LineFormatter($output, $dateFormat);
        //设置日志记录时间格式
        $stream = new StreamHandler($error_log, Logger::DEBUG);
        $stream->setFormatter($format);
        $log->pushHandler($stream);

        $error = [
            'database' => $header->getSchemaName(),
            'table' => $header->getTableName(),
            'bin-log' => $header->getLogfileName(),
            'offset' => $header->getLogfileOffset(),
            'eventType' => $this->event_arr[$eventType],
            'rowData' => $row_data,
            'error' => 'async data fail!'
        ];
        //保存首次同步失败记录
        $log->error(json_encode($error, JSON_UNESCAPED_UNICODE));

        //进行重试
        $result = false;
        switch ($eventType) {
            case EventType::INSERT :
                $result = $deal->insert($row_data);
                break;
            case EventType::DELETE :
                $result = $deal->delete($row_data);
                break;
            case EventType::UPDATE :
                $result = $deal->update($row_data);
        }

        //执行成功
        if ($result) {
            $success = [
                'database' => $header->getSchemaName(),
                'table' => $header->getTableName(),
                'bin-log' => $header->getLogfileName(),
                'offset' => $header->getLogfileOffset(),
                'eventType' => $this->event_arr[$eventType],
                'rowData' => $row_data,
                'describe' => 'first retry  async success!!!!'
            ];
            $log->info(json_encode($success, JSON_UNESCAPED_UNICODE));
            return ;
        }

        $dict = [
            1 => 'first',
            2 => 'second',
            3 => 'third',
            4 => 'fourth',
            5 => 'fifth',
            6 => 'sixth'
        ];
        //如果失败 则还要继续重试 从第二次开始
        for ($i = 2; $i <= $try_num; $i++) {
            //进行重试
            $result = false;
            switch ($eventType) {
                case EventType::INSERT :
                    $result = $deal->insert($row_data);
                    break;
                case EventType::DELETE :
                    $result = $deal->delete($row_data);
                    break;
                case EventType::UPDATE :
                    $result = $deal->update($row_data);
            }

            //重试成功
            if ($result) {
                $success = [
                    'database' => $header->getSchemaName(),
                    'table' => $header->getTableName(),
                    'bin-log' => $header->getLogfileName(),
                    'offset' => $header->getLogfileOffset(),
                    'eventType' => $this->event_arr[$eventType],
                    'rowData' => $row_data,
                    'describe' => $dict[$i].' retry  async success!!!!'
                ];

                $log->info(json_encode($success), JSON_UNESCAPED_UNICODE);
                break;
            } else {
                $error = [
                    'database' => $header->getSchemaName(),
                    'table' => $header->getTableName(),
                    'bin-log' => $header->getLogfileName(),
                    'offset' => $header->getLogfileOffset(),
                    'eventType' => $this->event_arr[$eventType],
                    'rowData' => $row_data,
                    'error' => $dict[$i].' retry data fail!'
                ];
                $log->emergency(json_encode($error), JSON_UNESCAPED_UNICODE);
            }
        }

    }
}