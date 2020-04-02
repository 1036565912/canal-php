<?php

namespace Example;

use Chenlin\CanalPhp\Action\BaseAction;
use Com\Alibaba\Otter\Canal\Protocol\Entry;
use Com\Alibaba\Otter\Canal\Protocol\EntryType;
use Com\Alibaba\Otter\Canal\Protocol\EventType;
use Com\Alibaba\Otter\Canal\Protocol\RowChange;
use Com\Alibaba\Otter\Canal\Protocol\RowData;
use Chenlin\CanalPhp\DealInterface\DealInterface;

class DemoAction extends BaseAction
{
    /**
     * @param DealInterface $deal
     * @throws \Exception
     */
    public function async(DealInterface $deal)
    {
        /** @var Entry $entry */
        foreach ($this->entry_list as $entry) {
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
                //进行事件判断
                switch ($evenType) {
                    case EventType::DELETE:
                        echo '删除数据同步'.PHP_EOL;
                        $result = $deal->delete($this->dealColumn($rowData->getBeforeColumns()));
                        break;
                    case EventType::INSERT:
                        echo '新增数据同步'.PHP_EOL;
                        $result = $deal->insert($this->dealColumn($rowData->getAfterColumns()));
                        break;
                    default:
                        //默认则是修改
                        echo '更新数据同步'.PHP_EOL;
                        $result = $deal->update($this->dealColumn($rowData->getAfterColumns()));
                }
            }
        }
    }
}