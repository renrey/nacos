/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.naming.healthcheck.heartbeat;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.naming.PreservedMetadataKeys;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.trace.DeregisterInstanceReason;
import com.alibaba.nacos.common.trace.event.naming.DeregisterInstanceTraceEvent;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.event.client.ClientOperationEvent;
import com.alibaba.nacos.naming.core.v2.event.metadata.MetadataEvent;
import com.alibaba.nacos.naming.core.v2.index.ClientServiceIndexesManager;
import com.alibaba.nacos.naming.core.v2.metadata.InstanceMetadata;
import com.alibaba.nacos.naming.core.v2.metadata.NamingMetadataManager;
import com.alibaba.nacos.naming.core.v2.pojo.HealthCheckInstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.InstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.misc.GlobalConfig;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.sys.utils.ApplicationUtils;

import java.util.Optional;

/**
 * Instance beat checker for expired instance.
 *
 * <p>Delete the instance if has expired.
 *
 * @author xiweng.yy
 */
public class ExpiredInstanceChecker implements InstanceBeatChecker {
    
    @Override
    public void doCheck(Client client, Service service, HealthCheckInstancePublishInfo instance) {
        // 是否开启过期淘汰
        boolean expireInstance = ApplicationUtils.getBean(GlobalConfig.class).isExpireInstance();
        // 判断是否超过配置的超时preserved.ip.delete.timeout可以删除
        if (expireInstance && isExpireInstance(service, instance)) {
            // 当前实例可以过期下线
            deleteIp(client, service, instance);
        }
    }
    
    private boolean isExpireInstance(Service service, HealthCheckInstancePublishInfo instance) {
        long deleteTimeout = getTimeout(service, instance);
        return System.currentTimeMillis() - instance.getLastHeartBeatTime() > deleteTimeout;
    }
    
    private long getTimeout(Service service, InstancePublishInfo instance) {
        // 优先使用元数据配置（实时，界面修改）的超时
        Optional<Object> timeout = getTimeoutFromMetadata(service, instance);
        if (!timeout.isPresent()) {
            // 没有在使用上次发布（心跳）时
            timeout = Optional.ofNullable(instance.getExtendDatum().get(PreservedMetadataKeys.IP_DELETE_TIMEOUT));
        }
        return timeout.map(ConvertUtils::toLong).orElse(Constants.DEFAULT_IP_DELETE_TIMEOUT);
    }
    
    private Optional<Object> getTimeoutFromMetadata(Service service, InstancePublishInfo instance) {
        Optional<InstanceMetadata> instanceMetadata = ApplicationUtils.getBean(NamingMetadataManager.class)
                .getInstanceMetadata(service, instance.getMetadataId());
        return instanceMetadata.map(metadata -> metadata.getExtendData().get(PreservedMetadataKeys.IP_DELETE_TIMEOUT));
    }
    
    private void deleteIp(Client client, Service service, InstancePublishInfo instance) {
        Loggers.SRV_LOG.info("[AUTO-DELETE-IP] service: {}, ip: {}", service.toString(), JacksonUtils.toJson(instance));
        // 对节点的客户端移除service（好像没啥用？）
        client.removeServiceInstance(service);
        /**
         * 下线
         * @see ClientServiceIndexesManager#handleClientOperation(ClientOperationEvent)
         */
        NotifyCenter.publishEvent(new ClientOperationEvent.ClientDeregisterServiceEvent(service, client.getClientId()));
        /**
         * 实例元数据数据过期！！(其实就是清除元数据，实际加入到过期集合等待清除)
         * @see NamingMetadataManager#onEvent(Event)
         */
        NotifyCenter.publishEvent(new MetadataEvent.InstanceMetadataEvent(service, instance.getMetadataId(), true));
        // 追溯相关，记录下线原因：心跳超时
        NotifyCenter.publishEvent(new DeregisterInstanceTraceEvent(System.currentTimeMillis(), "",
                false, DeregisterInstanceReason.HEARTBEAT_EXPIRE, service.getNamespace(), service.getGroup(),
                service.getName(), instance.getIp(), instance.getPort()));
    }
}
