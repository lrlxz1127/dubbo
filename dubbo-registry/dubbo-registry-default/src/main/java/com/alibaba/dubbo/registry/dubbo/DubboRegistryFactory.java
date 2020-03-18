/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.dubbo.registry.dubbo;

import com.alibaba.dubbo.common.Constants;
import com.alibaba.dubbo.common.URL;
import com.alibaba.dubbo.common.bytecode.Wrapper;
import com.alibaba.dubbo.common.utils.NetUtils;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.alibaba.dubbo.registry.Registry;
import com.alibaba.dubbo.registry.RegistryService;
import com.alibaba.dubbo.registry.integration.RegistryDirectory;
import com.alibaba.dubbo.registry.support.AbstractRegistryFactory;
import com.alibaba.dubbo.rpc.Invoker;
import com.alibaba.dubbo.rpc.Protocol;
import com.alibaba.dubbo.rpc.ProxyFactory;
import com.alibaba.dubbo.rpc.cluster.Cluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * DubboRegistryFactory
 *
 */
public class DubboRegistryFactory extends AbstractRegistryFactory {

	private Protocol protocol;// 协议
	private ProxyFactory proxyFactory;// 代理工厂
	private Cluster cluster;// 集群模式

	private static URL getRegistryURL(URL url) {
		return url.setPath(RegistryService.class.getName()).removeParameter(Constants.EXPORT_KEY)
				.removeParameter(Constants.REFER_KEY)
				.addParameter(Constants.INTERFACE_KEY, RegistryService.class.getName())
				.addParameter(Constants.CLUSTER_STICKY_KEY, "true").addParameter(Constants.LAZY_CONNECT_KEY, "true")
				.addParameter(Constants.RECONNECT_KEY, "false").addParameterIfAbsent(Constants.TIMEOUT_KEY, "10000")
				.addParameterIfAbsent(Constants.CALLBACK_INSTANCES_LIMIT_KEY, "10000")
				.addParameterIfAbsent(Constants.CONNECT_TIMEOUT_KEY, "10000")
				.addParameter(Constants.METHODS_KEY, StringUtils.join(
						new HashSet<String>(
								Arrays.asList(Wrapper.getWrapper(RegistryService.class).getDeclaredMethodNames())),
						","))
				// .addParameter(Constants.STUB_KEY, RegistryServiceStub.class.getName())
				// .addParameter(Constants.STUB_EVENT_KEY, Boolean.TRUE.toString()) //for event
				// dispatch
				// .addParameter(Constants.ON_DISCONNECT_KEY, "disconnect")
				.addParameter("subscribe.1.callback", "true").addParameter("unsubscribe.1.callback", "false");
	}

	public void setProtocol(Protocol protocol) {
		this.protocol = protocol;
	}

	public void setProxyFactory(ProxyFactory proxyFactory) {
		this.proxyFactory = proxyFactory;
	}

	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
	}

	@Override
	public Registry createRegistry(URL url) {
		url = getRegistryURL(url);// 得到注册url
		List<URL> urls = new ArrayList<URL>();
		urls.add(url.removeParameter(Constants.BACKUP_KEY));
		String backup = url.getParameter(Constants.BACKUP_KEY);// 得到备份url
		if (backup != null && backup.length() > 0) {
			String[] addresses = Constants.COMMA_SPLIT_PATTERN.split(backup);
			for (String address : addresses) {
				urls.add(url.setAddress(address));
			}
		}

		// 构建数据字典
		RegistryDirectory<RegistryService> directory = new RegistryDirectory<RegistryService>(RegistryService.class,
				url.addParameter(Constants.INTERFACE_KEY, RegistryService.class.getName())
						.addParameterAndEncoded(Constants.REFER_KEY, url.toParameterString()));
		// 得到集群Invoker
		Invoker<RegistryService> registryInvoker = cluster.join(directory);
		// 通过代理工厂得到注册服务
		RegistryService registryService = proxyFactory.getProxy(registryInvoker);
		// 构建dubbo注册服务，内部实现与注册中心连接
		DubboRegistry registry = new DubboRegistry(registryInvoker, registryService);
		directory.setRegistry(registry);
		directory.setProtocol(protocol);
		directory.notify(urls);
		directory.subscribe(new URL(Constants.CONSUMER_PROTOCOL, NetUtils.getLocalHost(), 0,
				RegistryService.class.getName(), url.getParameters()));
		return registry;
	}
}
