/**
 * 
 */
package com.chinacloud.incetl;

import java.util.Map;

import com.google.common.collect.Maps;

/**
 * 
 * @author nivalsoul kskscr@163.com
 * @date 2017年2月9日 下午2:00:57
 *
 */
public class Constant {

	public static Map<String, String> incType = Maps.newHashMap();
	
	static{
		incType.put("数值", "append");
		incType.put("时间戳","lastmodify");
	}
}
