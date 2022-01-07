# ‐*‐ coding: utf‐8 ‐*‐
"""
功能:
   指定几个【顶级】域名,分关键词种类监控首页词数
提示:
  不含xx%的人还搜了
  含微信,搜狗百科,--相关xx
结果:
	sgmo1_index_info.txt:各监控站点词的排名及url,如有2个url排名,只取第一个
	sgmo1_index_all.txt:每个kwd结果页所有url
"""

import requests,uuid
from pyquery import PyQuery as pq
import threading
import queue
from urllib.parse import unquote
import time
import gc
import random
import copy
import re
import tld
import traceback
import pandas as pd
requests.packages.urllib3.disable_warnings()


my_header = {
	'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
	'Accept-Encoding': 'gzip, deflate',
	'Accept-Language': 'zh-CN,zh;q=0.9',
	'Connection': 'keep-alive',
	'Host': 'm.sogou.com',
	'Sec-Fetch-Dest': 'document',
	'Sec-Fetch-Mode': 'navigate',
	'Sec-Fetch-Site': 'same-origin',
	'Sec-Fetch-User': '?1',
	'Upgrade-Insecure-Requests': '1',
	'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.' + str(uuid.uuid4()) + ' Safari/537.36',
}


def get_cookie(num):
	if num == 1:
		r = requests.get(url="https://wap.sogou.com",headers=my_header)
	else:
		r = requests.get(url="https://v.sogou.com/v?ie=utf8&query=&p=40030600",headers=my_header)
	cookie = ";".join([f'{key}={value}' for key, value in r.cookies.items()])
	return cookie


class sgmoIndexMonitor(threading.Thread):

	def __init__(self):
		threading.Thread.__init__(self)

	@staticmethod
	def read_excel(filepath):
		q = queue.Queue()
		df_dict = pd.read_excel(filepath,sheet_name=None)
		for sheet_name,df_sheet in df_dict.items():
			values = df_sheet['kwd'].dropna().values
			for kwd in values:
				if str(kwd).strip():
					q.put((sheet_name,kwd))
		return q

	# 获取某词serp源码
	def get_html(self,url,retry=1):
		headers = copy.deepcopy(my_header)
		headers['Cookie'] = get_cookie(retry)
		try:
			r = requests.get(url=url,headers=headers,verify=False,timeout=15)
		except Exception as e:
			print('获取源码失败',e)
			time.sleep(30)
			if retry > 0:
				self.get_html(url,retry-1)
		else:
			html = r.content.decode('utf-8',errors='ignore')
			url = r.url
			return html,url


	# 获取某词serp源码url
	def get_serp_urls(self,html):
		url_list = []
		doc = pq(html)
		# 搜了还搜会占一个排名值
		div_list = doc('div.results div.vrResult').items()
		for div in div_list:
			a = div('h3 a')
			if a:
				link = a.attr('href')
				if not link:
					url = None
				else:
					if 'javascript:' in str(link):
						url = 'https://wap.sogou.com/'
					else:
						re_url_obj = re.search('.*url=(.*)',link)
						page_url = re_url_obj.group(1) if re_url_obj else None
						url = unquote(page_url) if page_url else None
			else: # -相关xx
				span = div('h3 span')
				url = 'https://wap.sogou.com/' if span and 'resultLink' in str(span.attr('class')) else None
			# 提取排名
			rank_str = div.attr('id')
			rank_id = rank_str.split('_')[-1] if rank_str and rank_str.split('_') else None
			rank = int(rank_id) if rank_id else None
			if url and isinstance(rank,int):
				url_list.append((url,rank))
		return url_list


	# 提取url顶级域名
	def get_top_domain(self,real_url):
		top_domain = ''
		try:
			obj = tld.get_tld(real_url,as_object=True)
			top_domain = obj.fld
		except Exception as e:
			print('top domain parse error:',e)
		return top_domain


	# 首页排名url转为顶级域名信息
	def get_top_domains(self,real_urls_rank):
		domain_url_dicts = {}
		for real_url, my_order in real_urls_rank:
			if real_url:
				top_domain = self.get_top_domain(real_url)
				# 一个词某域名多个url有排名,算一次
				domain_url_dicts[top_domain] = (real_url,my_order) if top_domain not in domain_url_dicts else domain_url_dicts[top_domain]
		return domain_url_dicts


	# 保存结果数据
	def save(self,kwd,group,domain_url_dicts):
		# 目标域名是否出现
		for domain in TargetDomains:
			if domain not in domain_url_dicts:
				Lock.acquire()
				f.write(f'{kwd}\t无\t无\t{group}\t{domain}\n')
				Lock.release()
			else:
				my_url,my_order = domain_url_dicts[domain]
				Lock.acquire()
				f.write(f'{kwd}\t{my_url}\t{my_order}\t{group}\t{domain}\n')
				Lock.release()
		f_all.flush()
		f.flush()


	# 线程函数
	def run(self):
		while 1:
			group_kwd = q.get()
			group,kwd = group_kwd
			print(group,kwd)
			try:
				url = f"https://wap.sogou.com/web/searchList.jsp?from=index&keyword={kwd}"
				html_now_url = self.get_html(url)
				if not html_now_url:
					q.put(group_kwd)
					continue
				html,now_url = html_now_url
				re_obj = re.search('<title>(.*?)</title>',html,re.S|re.I)
				title = re_obj.group(1) if re_obj else ''
				if '搜狗搜索' not in title or 'https://wap.sogou.com/web/searchList.jsp' not in now_url:
					q.put(group_kwd)
					print('sleep...',now_url,title)
					time.sleep(120)
					continue
				url_list_rank = self.get_serp_urls(html)
			except Exception as e:
				print(traceback.format_exc())
				traceback.print_exc(file=open(f'{today}log.txt', 'a'))
			else:
				print(len(url_list_rank))
				url_str = ''.join([f'{kwd}\t{url}\t{rank}\t{group}\n' for url,rank in url_list_rank])
				Lock.acquire()
				f_all.write(url_str)
				Lock.release()
				f_all.flush()
				# {'域名':(real_url,my_order)}
				domain_url_dicts = self.get_top_domains(url_list_rank)
				self.save(kwd,group,domain_url_dicts) if domain_url_dicts else domain_url_dicts
			finally:
				del kwd
				gc.collect()
				q.task_done()
				time.sleep(3)


if __name__ == "__main__":
	start = time.time()
	today = time.strftime('%Y%m%d',time.localtime())
	# list_headers = [i.strip() for i in open('ua_mo.txt','r',encoding='utf-8')]
	TargetDomains = ['5i5j.com','lianjia.com','ke.com','anjuke.com','fang.com'] # 目标域名
	q = sgmoIndexMonitor.read_excel('2021kwd_url_core_city.xlsx')  # 关键词队列及分类
	all_num = q.qsize() # 总词数
	f = open(f'{today}sgmo1_index_info.txt','w',encoding="utf-8")
	f_all = open(f'{today}sgmo1_index_all.txt','w',encoding="utf-8")
	Lock = threading.Lock()
	# 设置线程数
	for i in list(range(1)):
		t = sgmoIndexMonitor()
		t.setDaemon(True)
		t.start()
	q.join()
	f.close()
	f_all.close()
	# 统计查询成功的词数
	with open(f.name,'r',encoding='utf-8') as fp:
		success = int(sum(1 for x in fp)/len(TargetDomains))
	end = time.time()
	print('关键词共{0}个,查询成功{1}个,耗时{2}min'.format(all_num,success,(end - start) / 60))
